import datetime
import pymongo
from bson import Timestamp, ObjectId
import math
import time
import pandas as pd


class DataExtraction:

    def __init__(self, connection, db, extract_all:list=[]) -> None:
        #Fix  timestamp to know where timestamp will be pulled from
        self.db = db
        self.connection = connection
        self.extract_all = extract_all
        self.oplog_con = self.connection.local.oplog.rs
        self.database = self.connection[self.db]


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
        
        collection = self.database["metadata"]
        
        # Try to retrieve the last document
        last_document = collection.find_one(sort=[('_id', pymongo.DESCENDING)])

        # If the collection is empty or there are no documents, insert the dictionary
        if not last_document:
            #Create new document 
            last_run = Timestamp(time=math.ceil(time.time()), inc=0)
            last_document = {
                "timestamp": last_run, 
                "date": datetime.datetime.now()
                    }

            collection.insert_one(last_document)

        return last_document['timestamp']

        

    def handle_update_operation(self, doc, data_dict):
        collection_name = doc['ns'].split('.')[-1]
        if collection_name in data_dict.keys():
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
            df = self.database[collection_name].find({'_id': {"$in" : value}})
            for d in df:
                if collection_name in data_dict_insert.keys():
                    data_dict_insert[collection_name].append(d)
                                    
                else:
                    data_dict_insert[collection_name] = [d]

        return data_dict_insert

    
    def extract_oplog_data(self):
        last_time = self.get_timestamp()
        data_dict_insert = {}
        data_dict_update = {}

        extract_start_time = datetime.datetime.now()
        if self.extract_all is None:
            cursor = self.oplog_con.find({'ts': {'$gt': last_time}},
                            cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                            oplog_replay=True)
        else:
            extract_all = '|'.join(('^'+n) for n in self.extract_all)
            cursor = self.oplog_con.find({'ts': {'$gt': last_time},
                            'ns' :{'$regex' : extract_all}},
                            cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                            oplog_replay=True)
        while cursor.alive:
            for doc in cursor:
                if doc['op'] == 'u':
                    self.handle_update_operation(doc=doc, data_dict=data_dict_update)
                else:
                    self.handle_insert_operation(doc=doc, data_dict=data_dict_insert)

            if (datetime.datetime.now() - extract_start_time).total_seconds() > (60*60)*10:
                break
            break
        
        data_dict_update = self.fix_duplicate_ids(data_dict_update)
        data_dict_insert = self.remove_duplicate_docs(data_dict_insert, data_dict_update)

        enitre_doc = self.extract_entire_doc_from_update(data_dict_update, data_dict_insert)

        #Recording metrics for extract metadata
        # extract_end_time = datetime.now() - extract_start_time
        # table_lenght = len(data_insert.keys())
        

        # # Need ideas on this alert logic : Do we make it mandatory for users to have an alert?
        # Alert.email()
        collection_df = {}
        for k, v in enitre_doc.items():
            collection_df[k] = pd.json_normalize(v)
            for col in collection_df[k].columns:
                if type(collection_df[k][col].iloc[0]) == ObjectId:
                    collection_df[k][col] = [str(line) for line in collection_df[k][col]]
        

        return collection_df