import datetime
import logging
from collections import defaultdict

import pandas as pd
import pymongo
from bson import ObjectId, Timestamp

from .core.run_state import MongoRunStateStore
from .sources.mongo_oplog import MongoOplogExtractor
from .systems.util import validate_date_format


class DataExtraction:
    """Extract recently inserted or modified data from MongoDB via the oplog."""

    def __init__(self, connection, db: str, backfill=None, extract_all: list = None) -> None:
        self.db = db
        self.connection = connection
        self.extract_all = extract_all if extract_all is not None else []
        self.oplog_con = self.connection.local.oplog.rs
        self.database = self.connection[self.db]
        self.backfill = backfill
        self._run_state = MongoRunStateStore(connection)
        self._extractor = MongoOplogExtractor(connection, db, self.extract_all)

    def handle_update_operation(self, doc, data_dict):
        collection_name = doc["ns"].split(".")[-1]
        data_dict[collection_name].append(doc["o2"]["_id"])

    def handle_insert_operation(self, doc, data_dict):
        df_dict = doc.get("o")
        collection_name = doc["ns"].split(".")[-1]
        data_dict[collection_name].append(df_dict)

    def fix_duplicate_ids(self, data_dict_update):
        return {key: list(set(value)) for key, value in data_dict_update.items()}

    def remove_duplicate_docs(self, data_dict_insert, data_dict_update):
        for key in data_dict_insert.keys():
            if key in data_dict_update:
                unique_ids = set(data_dict_update[key])
                data_dict_insert[key] = [
                    doc for doc in data_dict_insert[key] if doc["_id"] not in unique_ids
                ]
        return data_dict_insert

    def extract_entire_doc_from_update(self, data_dict_update, data_dict_insert):
        for key, value in data_dict_update.items():
            for doc in self.database[key].find({"_id": {"$in": value}}):
                data_dict_insert[key].append(doc)
        return data_dict_insert

    def extract_oplog_data(self):
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
        )
        logging.info("Data extraction started")

        if self.backfill is not None:
            validate_date_format(self.backfill)
            parsed_date = datetime.datetime.strptime(self.backfill, "%Y/%m/%d")
            filter_time = Timestamp(parsed_date, inc=0)
        else:
            filter_time = self._run_state.get_oplog_timestamp(self.db)

        data_dict_insert = defaultdict(list)
        data_dict_update = defaultdict(list)
        extract_start_time = datetime.datetime.now()

        if not self.extract_all:
            cursor = self.oplog_con.find(
                {"ts": {"$gt": filter_time}},
                cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                oplog_replay=True,
            )
        else:
            extract_all = "|".join("^" + f"{self.db}.{n}" for n in self.extract_all)
            cursor = self.oplog_con.find(
                {"ts": {"$gt": filter_time}, "ns": {"$regex": extract_all}},
                cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                oplog_replay=True,
            )

        while cursor.alive:
            for doc in cursor:
                if doc["op"] == "u":
                    self.handle_update_operation(doc=doc, data_dict=data_dict_update)
                else:
                    self.handle_insert_operation(doc=doc, data_dict=data_dict_insert)

            if (datetime.datetime.now() - extract_start_time).total_seconds() > 3600:
                break
            break

        data_dict_update = self.fix_duplicate_ids(data_dict_update)
        data_dict_insert = self.remove_duplicate_docs(data_dict_insert, data_dict_update)
        entire_doc = self.extract_entire_doc_from_update(data_dict_update, data_dict_insert)

        collection_df = {}
        for k, v in entire_doc.items():
            if k not in ("metadata", "$cmd") and v:
                collection_df[k] = pd.json_normalize(v, max_level=0)
                for col in collection_df[k].columns:
                    if len(collection_df[k]) and isinstance(collection_df[k][col].iloc[0], ObjectId):
                        collection_df[k][col] = [str(line) for line in collection_df[k][col]]

        logging.info("Data extraction ended")
        return collection_df
