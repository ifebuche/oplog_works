import datetime
import pymongo
from bson import ObjectId
import pandas as pd
from .systems.util import get_timestamp, append_timestamp


class DataExtraction:
    """Extract recently inserted or modified data from source with duplicates removed
    """

    def __init__(self, connection, db, extract_all:list=[]) -> None:
        #Fix  timestamp to know where timestamp will be pulled from
        self.db = db
        self.connection = connection
        self.extract_all = extract_all
        self.oplog_con = self.connection.local.oplog.rs
        self.database = self.connection[self.db]

    def handle_update_operation(self, doc, data_dict):
        """Extract Updated document from the cursor

        Args:
            doc (json): mongodb single document
            data_dict (dict): an empty dict to hold updated
        """
        collection_name = doc['ns'].split('.')[-1]
        if collection_name in data_dict.keys():
            data_dict[collection_name].append(doc['o2']['_id'])
        else:
            data_dict[collection_name] = [doc['o2']['_id']]

    def handle_insert_operation(self, doc, data_dict): 
        """Extract inserted document from the cursor

        Args:
            doc (json): mongodb single document
            data_dict (dict): an empty dict to hold updated
        """ 
        df_dict = doc.get('o')
        collection_name = doc['ns'].split('.')[-1]
        if collection_name in data_dict.keys():
            data_dict[collection_name].append(df_dict)
        else:
            data_dict[collection_name] = [df_dict]

    #Delete operation can easily be added 

    def fix_duplicate_ids(self, data_dict_update):
        """Removes the duplicate records in updates

        Desc:
            records with multiple updates will be trimmed to include last updated

        Args:
            data_dict_update (json): mongodb single document
        """ 
        return {key: list(set(value)) for key, value in data_dict_update.items()}
    
    def remove_duplicate_docs(self, data_dict_insert, data_dict_update):
        """Remove id from insert present in update

        Args:
            data_dict_insert (dict): holds the recently inserted document
            data_dict_update (dict): holds the recently updated document

        Returns:
            data_dict_insert (dict): modified recently inserted document
        """
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
        """combines insert and update into single dictionary

        Args:
            data_dict_insert (dict): holds the modified recently inserted document
            data_dict_update (dict): holds the recently updated document

        Returns:
            data_dict_insert (dict): combined dictionary
        """
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
        """Tail Oplog for recently inserted/modified data.

        This function monitors the MongoDB Oplog (operations log) to capture recently inserted or modified data in the 
        database collections. The function returns a dictionary containing the collection name as the key and the new data 
        as the value in the form of a DataFrame.

        Returns:
            collection_df (dict): A dictionary with the collection name as the key and the new data as the value in the 
            form of a DataFrame.
        """

        append_timestamp(self.connection)
        
        last_time = get_timestamp(self.connection)

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
            collection_df[k] = pd.json_normalize(v,max_level=0)
            for col in collection_df[k].columns:
                if type(collection_df[k][col].iloc[0]) == ObjectId:
                    collection_df[k][col] = [str(line) for line in collection_df[k][col]]

        return collection_df