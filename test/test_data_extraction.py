import unittest
import pymongo
import sys
import os
import json
import pandas as pd
from random import randint
from dotenv import find_dotenv, load_dotenv
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from MI_ETL.data_extraction import DataExtraction

load_dotenv(find_dotenv())
class TestDataExtraction(unittest.TestCase):

    @classmethod
    def setUpClass(cls):

        cls.client = pymongo.MongoClient('mongodb+srv://joe:R17meEbZWC3ZEcHe@cluster0.oogfs.mongodb.net/?retryWrites=true&w=majority')
        cls.db_name = "josephsTest2"
        cls.collection_name = "user" + str(randint(25,2000))
        cls.client.drop_database(cls.db_name)
        cls.db = cls.client[cls.db_name]
        cls.collection = cls.db[cls.collection_name]
        cls.test_documents = [{'_id': 'bhjbdjhdjs', 'data': 'test1'},{'_id': 'kjbdkjanjkankjn', 'data': 'test2'}]
        col = cls.collection.insert_many(cls.test_documents)
        
        
    def tearDown(self):
        self.client.drop_database(self.db_name)

    def test_data_extraction(self):
    

        data = list(self.collection.find({}))
        print('Mongo Data', data)

        extractor = DataExtraction(connection=self.client, db=self.db_name)
        extracted_data = extractor.extract_oplog_data()  
        dt = json.loads(extracted_data[self.collection_name].to_json(orient = "records"))
        print('olpog insert', dt)
        
        # self.assertEqual(dt, [self.test_documents])
    
    def test_data_extraction_update(self):
        
        self.collection.update_one({
                                '_id': 'bhjbdjhdjs'
                                },{
                                '$set': {
                                    'data': 'updated_test1'
                                }
                                }, upsert=True)
        
        extractor = DataExtraction(connection=self.client, db=self.db_name)
        extracted_data = extractor.extract_oplog_data()  
        dt = json.loads(extracted_data[self.collection_name].to_json(orient = "records"))
        print('oplog update', dt)
        new_test_documents = [{'_id': 1, 'data': 'updated_test1'},{'_id': 2, 'data': 'test2'}]
        # self.assertEqual(dt, new_test_documents)

    #did not want to go through the hassle of making this run last,, so i spelt it like how bubu would pronouce delete 
    def test_data_extraction_zelete(self):

        self.collection.delete_one({'_id': 'bhjbdjhdjs'})

        extractor = DataExtraction(connection=self.client, db=self.db_name)
        extracted_data = extractor.extract_oplog_data()
        dt = json.loads(extracted_data[self.collection_name].to_json(orient="records"))
        print('oplog delete', dt)
        expected_result = []

        # self.assertEqual(dt, expected_result)



if __name__ == '__main__':
    unittest.main()

