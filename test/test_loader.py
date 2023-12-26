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
from MI_ETL.loader import Loader

load_dotenv(find_dotenv())

print(os.getenv('mongo_conn','cant find mongo con'))
breakpoint()


class TestDataLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.client = pymongo.MongoClient(os.getenv("mongo_conn"))
        dataframe = pd.DataFrame(data=[1], columns=["_id"])
        cls.name = "user" + str(randint(25, 2000))
        print(cls.name)
        cls.final = {cls.name: dataframe}
        cls.loader = Loader(cls.client, cls.final)

    @classmethod
    def tearDownClass(cls):
        # delete tables
        pass

    def test_loader_fresh_table(self):
        print("--------------FRESH------------------")
        self.loader.run(
            warehouse=True,
            host=os.getenv("host"),
            user=os.getenv("user"),
            password=os.getenv("password"),
            db=os.getenv("db"),
            port=5432,
        )
        # assert the new table got created and have the inserted data

    def test_loader_updated_table(self):
        print("--------------UPDATED------------------")
        new_row = {"_id": 2}
        self.final[self.name] = self.final[self.name].append(new_row, ignore_index=True)
        self.loader = Loader(self.client, self.final)
        self.loader.run(
            schema_on_conflict="PASS",
            warehouse=True,
            host=os.getenv("host"),
            user=os.getenv("user"),
            password=os.getenv("password"),
            db=os.getenv("db"),
            port=5432,
        )

        # assert it has the new row and previous row is intact

    def test_loader_new_column_pass(self):
        print("--------------new column pass------------------")
        new_row = {"_id": 3, "new_col": "data"}
        self.final[self.name] = self.final[self.name].append(new_row, ignore_index=True)
        self.loader = Loader(self.client, self.final)
        self.loader.run(
            schema_on_conflict="PASS",
            warehouse=True,
            host=os.getenv("host"),
            user=os.getenv("user"),
            password=os.getenv("password"),
            db=os.getenv("db"),
            port=5432,
        )
        # assert new row was inserted , the code did not fail, and no new column were created

    def test_loader_new_column_fail(self):
        print("-------------COLUMN FAIL---------------------")
        new_row = {"_id": 4, "new_col": "data"}
        self.final[self.name] = self.final[self.name].append(new_row, ignore_index=True)
        self.loader = Loader(self.client, self.final)
        try:
            self.loader.run(
                schema_on_conflict="FAIL",
                warehouse=True,
                host=os.getenv("host"),
                user=os.getenv("user"),
                password=os.getenv("password"),
                db=os.getenv("db"),
                port=5432,
            )
            # raise a test fail
        except Exception as e:
            print(e)
            # test pass

    def test_loader_doesnt_allow_zduplicates(self):
        dataframe = pd.DataFrame(data=[2], columns=["_id"])
        final = {self.name: dataframe}
        loader = Loader(self.client, final)
        self.loader.run(
            schema_on_conflict="PASS",
            warehouse=True,
            host=os.getenv("host"),
            user=os.getenv("user"),
            password=os.getenv("password"),
            db=os.getenv("db"),
            port=5432,
        )


if __name__ == "__main__":
    unittest.main()
