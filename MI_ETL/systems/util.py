import datetime
import math
import os
import time
from datetime import datetime as dt
from pymongo import DESCENDING
import pandas as pd
from bson import Timestamp
from sqlalchemy import text

from ..Error import OplogWorksError

# Environment variable for configuring the application's environment
environment = os.getenv("ENVIRONMENT")

# Explanation regarding the presence of two similar functions
"""
-> You'll find two function bearing the same name, with one being commented out. 
-> Here's why : 1 writes and pull time from the date.csv on my local machine.
        this is because, if a recent run is inserted to batch_pipe_run_times in mongo,
        I might need to make quick changes and rerun. meaning since it used the time of last run,
        i might not get any change in small a time frame.

        The other functions write to mongo
"""

def update_loader_status(mongo_conn):
    """
    Update the loader status in the 'metadata' collection of MongoDB.

    Args:
        mongo_conn (pymongo.MongoClient): The MongoDB connection object.
    """
    db = mongo_conn["metadata"]
    collection = db["metadata"]
    collection.find_one_and_update(
        filter={},
        sort=[("_id", DESCENDING)],
        update={"$set": {"loader": True}},
        upsert=True,
    )


def get_timestamp(mongo_conn):
    """
    Retrieves the most recent timestamp from the 'metadata' collection in MongoDB.

    Args:
        mongo_conn (pymongo.MongoClient): The MongoDB connection object.

    Returns:
        bson.Timestamp: The most recent timestamp from the 'metadata' collection.
    """
    db = mongo_conn["metadata"]
    collection = db["metadata"]
    # Retrieve the last document based on loader status
    last_document = collection.find_one(
        filter={"loader": True}, sort=[("_id", DESCENDING)]
    )

    # Insert new document if collection is empty
    if not last_document:
        last_run = Timestamp(time=math.ceil(time.time()), inc=0)
        last_document = {
            "timestamp": last_run,
            "date": datetime.datetime.now(),
            "loader": False,
        }
        collection.insert_one(last_document)

    return last_document["timestamp"]


def append_timestamp(mongo_conn):
    """
    Appends a new timestamp entry to the 'metadata' collection in MongoDB.

    Args:
        mongo_conn (pymongo.MongoClient): The MongoDB connection object.
    """
    db = mongo_conn["metadata"]
    collection = db["metadata"]
    current_time = Timestamp(time=math.ceil(time.time()), inc=0)
    new_document = {
        "timestamp": current_time,
        "date": datetime.datetime.now(),
        "loader": False,
    }
    collection.insert_one(new_document)


def validate_kwargs(kwargs, required_params, func_name):
    """
    Validates the presence of required parameters in a dictionary.

    Args:
        kwargs (dict): The dictionary of keyword arguments.
        required_params (list): A list of required parameter names.
        func_name (str): The name of the function for which the parameters are being validated.

    Raises:
        OplogWorksError: If any of the required parameters are missing.
    """
    missing_params = [param for param in required_params if param not in kwargs]
    if missing_params:
        raise OplogWorksError(func_name, f"{', '.join(missing_params)} are missing")


def validate_date_format(date_str: str):
    """
    Validates if the given date string matches the expected date format.

    Args:
        date_str (str): The date string to validate.

    Raises:
        ValueError: If the date string is not in the expected 'YYYY/MM/DD' format.
    """
    try:
        datetime.datetime.strptime(date_str, "%Y/%m/%d")
    except ValueError:
        raise ValueError(f"The date {date_str} is not in the 'YYYY/MM/DD' format.")


def schema_validation(table_name, engine, df):
    """
    Validates the schema of a DataFrame against a table schema in a SQL database.

    Args:
        table_name (str): The name of the table against which to validate the DataFrame's schema.
        engine (sqlalchemy.engine.Engine): The SQL database engine.
        df (pandas.DataFrame): The DataFrame to validate.

    Returns:
        pandas.DataFrame: The DataFrame aligned with the table schema.
        list: A list of columns that were dropped from the DataFrame during schema alignment.

    Raises:
        OplogWorksError: If there are columns in the DataFrame that do not exist in the table schema.
    """
    try:
        #Using sqlalchemy execute pre 2.0
        column_values = pd.read_sql(f"""select * from customers limit 1""", engine)
    except AttributeError:
        with engine.connect() as connection:
            result = connection.execute(text(f"""SELECT * FROM customers LIMIT 1"""))
            column_values = pd.DataFrame(result.fetchall(), columns=result.keys())
        

    # Convert SQL table column names to a list
    columns_list = column_values.columns.tolist()
    # Create an empty DataFrame with the SQL table column names
    schema_df = pd.DataFrame(columns=columns_list)

    # Identify missing and extra columns
    missing_in_df1 = set(schema_df.columns) - set(df.columns)
    columns_to_drop = set(df.columns) - set(schema_df.columns)

    # Report missing columns in the incoming DataFrame
    if missing_in_df1:
        print(
            "schema validation",
            f"Following columns are missing in incoming {' ,'.join(missing_in_df1)}",
        )

    # Handle extra columns in the incoming DataFrame
    if columns_to_drop:
        try:
            print(
                "schema validation",
                f"Following columns are extras {' ,'.join(columns_to_drop)}",
            )
            df.drop(columns=list(columns_to_drop), inplace=True)
        except KeyError:
            raise OplogWorksError(
                "schema validation",
                f"Following columns are not present in current schema {' ,'.join(columns_to_drop)}",
            )

    resolved_df = pd.concat([schema_df, df])

    return resolved_df, list(columns_to_drop)
