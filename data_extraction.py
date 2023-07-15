import os
import time
from datetime import datetime
import pymongo
from pymongo.cursor import Cursor 
from bson import ObjectId, Timestamp
from Connector import Connector
from Alert import Alert



class DataExtraction:

    def __init__(self, mongo_url, ts, extract_all:list=[]) -> None:
        self.mongo_url = mongo_url
        #Fix  timestamp to know where timestamp will be pulled from
        self.ts = ts
        self.extract_all = extract_all
        self.mongo_con = Connector.Source.mongo(self.mongo_url)
        self.oplog_con = self.mongo_con.local.oplog.rs


    def handle_update_operation(self, doc, data_dict):
        data_dict = {}
        collection_name = doc['ns'].split('.')[-1]
        if collection_name in data_dict:
            data_dict[collection_name].append(doc['o2']['_id'])
        else:
            data_dict[collection_name] = [doc['o2']['_id']]

    def handle_insert_operation(self, doc, data_dict):  
        df_dict = doc.get('o')
        collection_name = doc['ns'].split('.')[-1]
        if collection_name in data_dict.keys():
            data_dict[collection_name].append(df_dict)
        else:
            data_dict[collection_name] = [df_dict]

    #Delete operation can easily be added 

    def fix_duplicate_ids(self, data_dict_update):
        return {key: list(set(value)) for key, value in data_dict_update.items()}
    
    def remove_duplicate_docs(self, data_dict_insert, data_dict_update):
        for k, v in data_dict_update.items():
            final_inserts_list = []
            insert = data_dict_insert.get(k)
            if insert:
                for doc in insert:
                    if not doc['_id'] in v:
                        final_inserts_list.append(doc)
                data_dict_insert[k] = final_inserts_list
            
        return data_dict_insert
    

    
    def extract_entire_doc_from_update(self, data_dict_update, data_dict_insert):
        for key, value in data_dict_update.items():
            collection_name = key
            df = self.mongo_con[collection_name].find({'_id': {"$in" : value}})
            if df.count() > 0:
                for d in df:
                    if collection_name in data_dict_insert.keys():
                        data_dict_insert[collection_name].append(d)
                                        
                    else:
                        data_dict_insert[collection_name] = [d]

    
    def extract_oplog_data(self, ):
        data_dict_insert = {}
        data_dict_update = {}

        extract_start_time = datetime.now()
        if self.extract_all is None:
            cursor = self.oplog_con.find({'ts': {'$gt': self.ts}},
                            cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                            oplog_replay=True)
        else:
            extract_all = '|'.join(('^'+n) for n in self.extract_all)
            cursor = self.oplog_con.find({'ts': {'$gt': self.ts},
                            'ns' :{'$regex' : extract_all}},
                            cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                            oplog_replay=True)
        while cursor.alive:
            for doc in cursor:
                if doc['op'] == 'u':
                    self.handle_update_operation(doc, data_dict_update)
                else:
                    self.handle_insert_operation(doc, data_dict_insert)

            if (datetime.now() - extract_start_time).total_seconds() > (60*60)*10:
                break
            break
        
        data_dict_update = self.fix_duplicate_ids(data_dict_update)
        data_insert = self.remove_duplicate_docs(data_dict_insert, data_dict_update)

        #Recording metrics for extract metadata
        extract_end_time = datetime.now() - extract_start_time
        table_lenght = len(data_insert.keys())
        

        # Need ideas on this alert logic : Do we make it mandatory for uses to have an alert?
        Alert.email()
        
        
        return data_insert

    