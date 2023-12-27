import json
import os
import sys
import unittest
import certifi
from random import randint

import pandas as pd
import pymongo
from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine, inspect

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)
from MI_ETL.loader import Loader

load_dotenv(find_dotenv())

print(os.getenv('mongo_conn','cant find mongo con'))
breakpoint()


class TestLoader(unittest.TestCase):

    @classmethod
    def setup_postgresql(cls):
        cls.pg_conn = create_engine(
            f'postgresql://{os.getenv("user")}:{os.getenv("password")}@'
            f'{os.getenv("host")}:{os.getenv("port")}/'
            f'{os.getenv("db")}')
    @classmethod
    def setUpClass(cls):
        cls.mongo_client = pymongo.MongoClient(os.getenv("mongo_conn"), tlsCAFile=certifi.where())
        cls.test_dataframe = pd.DataFrame(data=[[1,2]], columns=["_id","data"])
        cls.test_table_name = "user" + str(randint(25, 2000))

        print(cls.test_table_name)
        cls.final = {cls.test_table_name: cls.test_dataframe}
        cls.loader = Loader(cls.mongo_client, cls.final)

        cls.setup_postgresql()
        with cls.pg_conn.connect() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {cls.test_table_name}")

    @classmethod
    def tearDownClass(cls):
         with cls.pg_conn.connect() as conn:
            conn.execute(f"DROP TABLE {cls.test_table_name}")
       
    

    def test_loader_fresh_table(self):
        print('----fresh table----')
        self.loader.run(warehouse=True, host=os.getenv("host"), user=os.getenv("user"),
                        password=os.getenv("password"), db=os.getenv("db"), port=5432)

        returned_df = pd.read_sql(f"SELECT * FROM {self.test_table_name}", self.pg_conn)
        self.assertTrue(returned_df.equals(self.test_dataframe))

    def test_loader_new_row(self):
        print('----new row----')
        new_row = pd.DataFrame([{"_id": 2, "data": 3}])
        #self.final[self.test_table_name] = self.final[self.test_table_name].append(new_row, ignore_index=True)
        self.final[self.test_table_name] = pd.concat([self.final[self.test_table_name], new_row])

        self.loader = Loader(self.mongo_client, self.final)
        self.loader.run(warehouse=True, host=os.getenv("host"), user=os.getenv("user"),
                        password=os.getenv("password"), db=os.getenv("db"), port=5432)

        returned_df = pd.read_sql(f"SELECT * FROM {self.test_table_name}", self.pg_conn)
        self.assertCountEqual(returned_df.values.tolist(), self.final[self.test_table_name].values.tolist())

    def test_loader_unknown_column(self):
        print('----unknown column----')
        new_row = pd.DataFrame([{"_id": 3, "data": 3, "new_col": "data"}])
        #self.final[self.test_table_name] = self.final[self.test_table_name].append(new_row, ignore_index=True)

        self.final[self.test_table_name] = pd.concat([self.final[self.test_table_name], new_row])
        
        self.loader = Loader(self.mongo_client, self.final)
        self.loader.run(warehouse=True, host=os.getenv("host"), user=os.getenv("user"),
                        password=os.getenv("password"), db=os.getenv("db"), port=5432)

        returned_df = pd.read_sql(f"SELECT * FROM {self.test_table_name} WHERE _id=3", self.pg_conn)
        self.assertTrue(len(returned_df) == 1 and 'new_col' not in returned_df.columns)

    def test_loader_null_missing_column_(self):
        print('----missing column----')
        new_row = pd.DataFrame([{"_id": 4}])
        #self.final[self.test_table_name] = self.final[self.test_table_name].append(new_row, ignore_index=True)

        self.final[self.test_table_name] = pd.concat([self.final[self.test_table_name], new_row])
        
        self.loader = Loader(self.mongo_client, self.final)
        self.loader.run(schema_on_conflict="FAIL", warehouse=True, host=os.getenv("host"), user=os.getenv("user"),
                        password=os.getenv("password"), db=os.getenv("db"), port=5432)

        returned_df = pd.read_sql(f"SELECT * FROM {self.test_table_name} WHERE _id=4", self.pg_conn)
        self.assertTrue(len(returned_df) == 1 and returned_df['data'].iloc[0] is None)

    def test_loader_id_duplicates(self):
        print('----duplicates----')
        dataframe = pd.DataFrame(data=[[1, 8]], columns=["_id", "data"])
        self.final[self.test_table_name] = dataframe

        self.loader = Loader(self.mongo_client, self.final)
        self.loader.run(warehouse=True, host=os.getenv("host"), user=os.getenv("user"),
                        password=os.getenv("password"), db=os.getenv("db"), port=5432)

        returned_df = pd.read_sql(f"SELECT * FROM {self.test_table_name} WHERE _id=1", self.pg_conn)
        self.assertTrue(len(returned_df) == 1 and returned_df['data'].iloc[0] == 8)

        


if __name__ == "__main__":
    unittest.main()
