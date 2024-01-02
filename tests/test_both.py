import json
import os
import sys
import unittest
import certifi
import json
from random import randint
from sqlalchemy import create_engine, inspect

import pandas as pd
import pymongo
from dotenv import find_dotenv, load_dotenv

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from MI_ETL.data_extraction import DataExtraction
from MI_ETL.loader import Loader

load_dotenv(find_dotenv())


class TestDataExtraction(unittest.TestCase):

    @classmethod
    def setup_mongodb(cls):
        cls.mongo_client = pymongo.MongoClient(os.getenv('mongo_conn'), tlsCAFile=certifi.where())
        cls.db_name = "josephsTest2"
        cls.collection_name = f"user{randint(25, 2000)}"
        cls.mongo_db = cls.mongo_client[cls.db_name]
        cls.collection = cls.mongo_db[cls.collection_name]
        cls.test_documents = [{"_id": "1", "data": "test1"}, {"_id": "2", "data": "test2"}]
        cls.collection.insert_many(cls.test_documents)
    
    @classmethod
    def setup_postgresql(cls):
        cls.pg_conn = create_engine(
            f'postgresql://{os.getenv("user")}:{os.getenv("password")}@'
            f'{os.getenv("host")}:{os.getenv("port")}/'
            f'{os.getenv("db")}'
        )
    @classmethod
    def setUpClass(cls):
        cls.setup_mongodb()
        cls.setup_postgresql()

    @classmethod
    def tearDownClass(cls):
        cls.mongo_client.drop_database(cls.db_name)

        with cls.pg_conn.connect() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {cls.collection_name}")


    def test_data_extraction(self):
        extractor = DataExtraction(connection=self.mongo_client, db=self.db_name)
        extracted_data = extractor.extract_oplog_data()
        extracted_data_for_test = json.loads(extracted_data[self.collection_name].to_json(orient="records"))
        final = {self.collection_name: extracted_data[self.collection_name]}
        print(final)
        loader = Loader(mongo_conn=self.mongo_client, data=final)
        loader.run(
            warehouse=True,
            host=os.getenv("host"),
            user=os.getenv("user"),
            password=os.getenv("password"),
            db=os.getenv("db"),
            port=os.getenv("port"),
        )

        self.assertEqual(len(extracted_data[self.collection_name]), 2)
        print('extracted----' , extracted_data_for_test)
        print('assert-------', self.test_documents)
        self.assertCountEqual(extracted_data_for_test, self.test_documents)

        inspector = inspect(self.pg_conn)
        self.assertIn(self.collection_name, inspector.get_table_names())

        with self.pg_conn.connect() as conn:
            result = conn.execute(f"SELECT * FROM {self.collection_name}").fetchall()
            print('--------result-------',result)
            data = [
                        dict(zip(['_id','data'], row))
                        for row in result
                    ]
            print(data)
            self.assertEqual(len(result), 2)
            self.assertCountEqual(data, self.test_documents)
        

if __name__ == "__main__":
    unittest.main()
