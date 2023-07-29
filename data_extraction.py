import os
import time
from datetime import datetime
import pymongo
import math
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

    def get_timestamp(self):
        """
        This method retrieves the most recent timestamp from the 'metadata' database in MongoDB.
        If the 'metadata' database does not exist, the method creates it and inserts a document
        with the current timestamp.

        Returns:
            last_run (bson.timestamp.Timestamp): The most recent timestamp from the 'metadata' 
                                                database, or the current timestamp if the 
                                                'metadata' database was just created.
        """
        
        if "metadata" in self.mongo_con.list_database_names():
            meta_data = self.mongo_con.metadata
            last_run_doc = meta_data.find().sort('date', pymongo.DESCENDING).limit(1)
            for doc in last_run_doc:
                last_run = doc['timestamp']
        else:   
            meta_data = self.mongo_con['metadata']
            last_run = Timestamp(time=math.ceil(time.time()), inc=0)
            document = {
                "timestamp": last_run,  
            }
            meta_data.insert_one(document)

        return last_run

        

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
        

        # Need ideas on this alert logic : Do we make it mandatory for users to have an alert?
        Alert.email()
        
        
        return data_insert
    