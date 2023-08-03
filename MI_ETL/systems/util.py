from bson import Timestamp
import os
import time
import datetime
from pymongo import DESCENDING
from numpy import insert
import pandas as pd
import math
import smtplib
from datetime import datetime as dt
from email.mime.text import MIMEText
from email.utils import formatdate
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText


environment = os.getenv('ENVIRONMENT')



"""
-> You'll find two function bearing the same name, with one being commented out. 
-> Here's why : 1 writes and pull time from the date.csv on my local machine.
        this is because , if a recent run is inserted to batch_pipe_run_times in mongo,
        I might need to make quick changes and rerun. meaning since it used the time of last run,
        i might not get any change in small a time frame.

        The other functions write to mongo
"""


def update_loader_status(mongo_conn):

    db = mongo_conn['metadata']
    collection = db['metadata']
    collection.find_one_and_update(filter= {},sort=[('_id', DESCENDING)],update={"$set":{"loader": True}},upsert=True)


def get_timestamp(mongo_conn):
    """
    This method retrieves the most recent timestamp from the 'metadata' database in MongoDB.
    If the 'metadata' database does not exist, the method creates it and inserts a document
    with the current timestamp.

    Returns:
        last_run (bson.timestamp.Timestamp): The most recent timestamp from the 'metadata' 
                                            database, or the current timestamp if the 
                                            'metadata' database was just created.
    """
    
    db = mongo_conn["metadata"]
    collection = db["metadata"]
    # Try to retrieve the last document
    last_document = collection.find_one(filter={'loader': True},sort=[('_id', DESCENDING)])

    # If the collection is empty or there are no documents, insert the dictionary
    if not last_document:
        #Create new document 
        last_run = Timestamp(time=math.ceil(time.time()), inc=0)
        last_document = {
            "timestamp": last_run, 
            "date": datetime.datetime.now(),
            "loader": False
                }

        collection.insert_one(last_document)

    return last_document['timestamp']

    
def append_timestamp(mongo_conn):
    """Append a new document with run time and status to the 'metadata' collection. 
    """
    db = mongo_conn["metadata"]
    collection = db["metadata"]
    current_time = Timestamp(time=math.ceil(time.time()), inc=0)
    new_document = {
            "timestamp": current_time, 
            "date": datetime.datetime.now(),
            "loader": False
                }

    collection.insert_one(new_document)
