import json
import os
import sys
import unittest
from random import randint

import pandas as pd
import pymongo
from dotenv import find_dotenv, load_dotenv

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from MI_ETL.data_extraction import DataExtraction

load_dotenv(find_dotenv())


class TestDataExtraction(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = pymongo.MongoClient(os.getenv("mongo_conn"))
        cls.db_name = "josephsTest" + str(randint(25, 2000))
        cls.collection_name = "user" + str(randint(25, 2000))

        cls.client.drop_database(cls.db_name)

        cls.db = cls.client[cls.db_name]
        cls.collection = cls.db[cls.collection_name]

        cls.test_documents = [
            {"_id": "bhjbdjhdjs", "data": "test1"},
            {"_id": "kjbdkjanjkankjn", "data": "test2"},
        ]
        col = cls.collection.insert_many(cls.test_documents)
    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(cls.db_name)

    def test_data_extraction(self):
        data = list(self.collection.find({}))
        print("Mongo Data", data)

        extractor = DataExtraction(connection=self.client, db=self.db_name, extract_all=[self.collection_name])
        extracted_data = extractor.extract_oplog_data()

        dt = json.loads(extracted_data[self.collection_name].to_json(orient="records"))
        print("olpog insert", dt)

        self.assertEqual(len(extracted_data[self.collection_name]), 2)
        print('extracted----' , dt) 
        print('assert-------', self.test_documents)
        self.assertCountEqual(dt, self.test_documents)

    def test_data_extraction_update(self):
        self.collection.update_one(
            {"_id": "bhjbdjhdjs"}, {"$set": {"data": "updated_test1"}}, upsert=True
        )
        print(self.db_name)
        
        extractor = DataExtraction(connection=self.client, db=self.db_name,extract_all=[self.collection_name])
        extracted_data = extractor.extract_oplog_data()
        dt = json.loads(extracted_data[self.collection_name].to_json(orient="records"))
        
        print("oplog update", dt)
        new_test_documents = [
            {"_id": "bhjbdjhdjs", "data": "updated_test1"},
            {"_id": "kjbdkjanjkankjn", "data": "test2"},
        ]
        self.assertCountEqual(dt, new_test_documents)

    # did not want to go through the hassle of making this run last,, so i spelt it like how bubu would pronouce delete
    def test_data_extraction_zelete(self):
        self.collection.delete_one({"_id": "bhjbdjhdjs"})

        extractor = DataExtraction(connection=self.client, db=self.db_name)
        extracted_data = extractor.extract_oplog_data()
        dt = json.loads(extracted_data[self.collection_name].to_json(orient="records"))
        print("oplog delete", dt)
        expected_result = [{"_id": "kjbdkjanjkankjn", "data": "test2"}]

        self.assertCountEqual(dt, expected_result)


if __name__ == "__main__":
    unittest.main()
