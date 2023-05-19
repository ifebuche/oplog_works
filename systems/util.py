from bson import Timestamp
import os
import time
from numpy import insert
import pandas as pd
import math
from systems.Connector import MongoConn
import pymongo, smtplib
import pandas as pd
from datetime import datetime as dt
from email.mime.text import MIMEText
from email.utils import formatdate
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText


environment = os.getenv('ENVIRONMENT')


mongo_client = MongoConn()
mongo_conn = mongo_client.kbanalytics
run_time = mongo_conn.batch_pipe_run_times

"""
-> You'll find two function bearing the same name, with one being commented out. 
-> Here's why : 1 writes and pull time from the date.csv on my local machine.
        this is because , if a recent run is inserted to batch_pipe_run_times in mongo,
        I might need to make quick changes and rerun. meaning since it used the time of last run,
        i might not get any change in small a time frame.

        The other functions write to mongo
"""

def get_last_run():
     """
     Get the time of last run from mongo
     """
     last_run = run_time.find().sort('date', pymongo.DESCENDING).limit(1) 
     ttime = pd.DataFrame(last_run).time[0]
     return ttime

def append_last_run(current_date,current_time, changed_tables, load_time,processing_time, len_table,extract_time, env=True):

    """
    *inserts a new record in batch_pipe_run_time mongo collection*
    changed_tables: str
        contains names of collection/table that had changes in a particular period of time
    len_tables: int
        contains the number of collection/table that had changes in a particular period of time
    extract_time: float
        time taken to extract data in all collections that had changes
    env: bool
        either in production(Flase) state or dev state 
    """

    body = {
        "date": current_date, "time": current_time,
        # "date": insert_datetime, "time": insert_time,
        "environment": env, "extract_time": extract_time,
        "process_time": processing_time,
        "load_time": load_time,
        "table_length": len_table, "tables": changed_tables
        }

    insert_result = run_time.insert_one(body)


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

