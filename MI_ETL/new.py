import datetime
import logging
from collections import defaultdict

import pandas as pd
import pymongo
from bson import ObjectId, Timestamp

from .systems.util import append_timestamp, get_timestamp, validate_date_format

class DataExtraction:
    """Class for extracting recently inserted or modified data from a MongoDB database with duplicates removed.

    This class tails the MongoDB operation logs (oplog) to capture recently inserted
    or modified data in the database collections and returns it in a structured format
    after removing any duplicates.

    Attributes:
        db (str): The database name to extract data from.
        connection (pymongo.MongoClient): The connection to MongoDB.
        extract_all (list): List of collection names to specifically extract data from.
                            If empty, data is extracted from all collections.
        oplog_con (pymongo.Collection): Connection to the MongoDB oplog for tailing operations.
        database (pymongo.database.Database): The specific database to work on.
        backfill (datetime or None): Specific start time for data extraction. If None,
            extraction starts from the last saved timestamp.

    Methods:
        handle_update_operation(doc, data_dict): Process an update operation from the oplog.
        handle_insert_operation(doc, data_dict): Process an insert operation from the oplog.
        fix_duplicate_ids(data_dict_update): Remove duplicate records in the updates.
        remove_duplicate_docs(data_dict_insert, data_dict_update): Remove duplicate documents from inserts.
        extract_entire_doc_from_update(data_dict_update, data_dict_insert): Combine insert and update documents.
        extract_oplog_data(): Tail the oplog for recently inserted/modified data.

    Method Used:
        extract_oplog_data()
    """



    def __init__(self, connection: pymongo.MongoClient, db: str, backfill=None, extract_all: list = []):
        """Initializes the DataExtraction with specified database and connection parameters.

        Args:
            connection (pymongo.MongoClient): The connection to MongoDB.
            db (str): The database name to extract data from.
            backfill (datetime, optional): Specific start time for data extraction. Defaults to None.
            extract_all (list, optional): List of collection names to specifically extract data from.
                If empty, data is extracted from all collections. Defaults to an empty list.
        """
        self.db = db
        self.connection = connection
        self.extract_all = extract_all
        self.oplog_con = self.connection.local.oplog.rs
        self.database = self.connection[self.db]
        self.backfill = backfill

    def handle_update_operation(self, doc: dict, data_dict: defaultdict):
        """Extracts updated document ID from the oplog document and appends it to the provided dictionary.

        Args:
            doc (dict): The oplog document representing an update operation.
            data_dict (defaultdict): The dictionary to append the document ID to.
        """
        collection_name = doc["ns"].split(".")[-1]
        data_dict[collection_name].append(doc["o2"]["_id"])

    def handle_insert_operation(self, doc: dict, data_dict: defaultdict):
        """Extracts inserted document from the oplog document and appends it to the provided dictionary.

        Args:
            doc (dict): The oplog document representing an insert operation.
            data_dict (defaultdict): The dictionary to append the document to.
        """
        df_dict = doc.get("o")
        collection_name = doc["ns"].split(".")[-1]
        data_dict[collection_name].append(df_dict)

    def fix_duplicate_ids(self, data_dict_update: defaultdict) -> dict:
        """Removes duplicate records in updates by ensuring only the last update is retained for each document.

        Args:
            data_dict_update (defaultdict): The dictionary containing updated document IDs.

        Returns:
            dict: A dictionary with duplicate update entries removed.
        """
        return {key: list(set(value)) for key, value in data_dict_update.items()}

    def remove_duplicate_docs(self, data_dict_insert: defaultdict, data_dict_update: defaultdict) -> defaultdict:
        """Removes any inserted documents that have been updated later to ensure only the latest version is kept.

        Args:
            data_dict_insert (defaultdict): Dictionary holding inserted documents.
            data_dict_update (defaultdict): Dictionary holding updated document IDs.

        Returns:
            defaultdict: The updated dictionary with duplicates removed from inserts.
        """
        for key in data_dict_insert.keys():
            if key in data_dict_update:
                unique_ids = set(data_dict_update[key])
                data_dict_insert[key] = [
                    doc for doc in data_dict_insert[key] if doc["_id"] not in unique_ids
                ]
        return data_dict_insert

    def extract_entire_doc_from_update(self, data_dict_update: defaultdict, data_dict_insert: defaultdict) -> defaultdict:
        """Combines documents from insert and update operations into a single dictionary.

        Args:
            data_dict_insert (defaultdict): Dictionary holding recently inserted documents.
            data_dict_update (defaultdict): Dictionary holding recently updated documents.

        Returns:
            defaultdict: The combined dictionary of inserted and updated documents.
        """
        for key, value in data_dict_update.items():
            collection_name = key
            df = self.database[collection_name].find({"_id": {"$in": value}})
            for d in df:
                data_dict_insert[collection_name].append(d)
        return data_dict_insert

    def extract_oplog_data(self) -> dict:
        """Tails the MongoDB Oplog to extract recently inserted or modified data.

        Monitors the MongoDB Oplog to capture recently inserted or modified data in the
        database collections and returns it in a structured format after removing any duplicates.

        Returns:
            dict: A dictionary with collection names as keys and lists of new data as values in DataFrame format.
        """
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(message)s",
            datefmt="%d-%b-%y %H:%M:%S",
        )
        logging.info("Data extraction started")
        append_timestamp(self.connection)

        last_time = get_timestamp(self.connection)
        data_dict_insert = defaultdict(list)
        data_dict_update = defaultdict(list)
        extract_start_time = datetime.datetime.now()

        filter_time = self.backfill if self.backfill else last_time
        cursor = self._define_cursor(filter_time)

        while cursor.alive:
            for doc in cursor:
                if doc["op"] == "u":
                    self.handle_update_operation(doc=doc, data_dict=data_dict_update)
                else:
                    self.handle_insert_operation(doc=doc, data_dict=data_dict_insert)

            if (datetime.datetime.now() - extract_start_time).total_seconds() > (60 * 60) * 10:
                break
            break

        data_dict_update = self.fix_duplicate_ids(data_dict_update)
        data_dict_insert = self.remove_duplicate_docs(data_dict_insert, data_dict_update)
        entire_doc = self.extract_entire_doc_from_update(data_dict_update, data_dict_insert)

        collection_df = self._normalize_data(entire_doc)

        logging.info("Data extraction ended")
        return collection_df

    # Placeholder for private methods that need to be implemented:
    # _define_cursor, _normalize_data
