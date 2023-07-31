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
    db = mongo_conn["metadata"]
    collection = db["metadata"]
    current_time = Timestamp(time=math.ceil(time.time()), inc=0)
    new_document = {
            "timestamp": current_time, 
            "date": datetime.datetime.now(),
            "loader": False
                }

    collection.insert_one(new_document)

if environment != 'highway':
    def get_last_run():
        """
        Get las run from local machine
        """
        if os.path.exists('date.csv'):
            date  = pd.read_csv('date.csv')
            date = date['date'].to_list()
            last_run = date[-1]
            return last_run
        else:
            date_df = pd.DataFrame([time.time()], columns= ['date'])
            date = date_df['date'].to_list()
            last_run = date[-1]
            date_df.to_csv('date.csv', index=False)
            return last_run

    def append_last_run():
        """
        *inserts a new record in local machine date.csv*
        """
        date = pd.read_csv('date.csv')
        new_date = pd.DataFrame({'date': time.time()}, index=[0])
        #print('In the append last run function', date)
        date = pd.concat([date, new_date], ignore_index=True)
        date.to_csv('date.csv', index=False)


def timestamp_to_bson_ts():
    """
    Function to convert timestamp to bson timestamp format which oplog uses
    """
    last_run = get_last_run()
    print('Last run was ', last_run)
    time_stamp = math.ceil(last_run)
    ts = Timestamp(time=time_stamp, inc= 0)
    return ts

#Mailer
def send_mail(message, subject='Success'):
    """
    Function to send out status mail to recipients
    """
    emailAdds = ['paschal.a@kobo360.com', 'ogbeide@kobo360.com']
    names = ['Paschal', 'Nelson']
    fromaddress = "paschal.a@kobo360.com"
    pass_w = os.environ['PASSWORD']

    for item, name in zip(emailAdds, names):
        fromaddr = fromaddress
        toaddr = item
        replyto = "paschal.a@kobo360.com"

        msg = MIMEMultipart()
        msg['From'] = "Data Engineering"
        msg['To'] = toaddr
        msg['In-Reply-To'] = replyto

        msg['Subject'] = f"Oplog Pipeline Cron: {subject}"
        if subject != 'Success':
            msg['Subject'] = f"Oplog Pipeline Cron: 'Failure'"
        
        body = message
        
        msg.attach(MIMEText(body, 'plain'))

        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(fromaddr, pass_w)
        text = msg.as_string()
        server.sendmail(fromaddress, toaddr, text)
        server.quit()
        print("Email sent to" + " " + name + " " + item)

