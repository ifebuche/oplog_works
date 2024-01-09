from pymongo import MongoClient
from sqlalchemy import create_engine
import os

open_machine = ""
environment = os.getenv('ENVIRONMENTx', open_machine)

if environment != 'highway':
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())

def MongoConn():

    """
    This creates a connection to kobo mongo db. 
    replace "MONGO_CONN" with your env variable name holding your credentials 
    """

    __mongo = None
    
    try:
        mongo_cre2 = os.getenv('MONGO_ANALYTICS_CONN_STRINGx', open_machine)
        __mongo = MongoClient(mongo_cre2)
       

    except Exception as e:
        print("Connection Failed")
        print("Error: ", e)

    return __mongo


def MongoOplog():
    __mongo = None
    __oplog = None

    try:
        __mongo = MongoConn()
        __oplog = __mongo.local.oplog.rs
        print("Connection to oplog made")
    
    except Exception as e:
        print(f"Could not connect to mongo oplog {e}")

    return __oplog

def WarehouseConn():
    # host = os.environ['DB_HOST']
    # user = os.environ['DB_USER']
    # password = os.environ['DB_PASS']

    host = os.getenv('DB_HOST')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASS')
    port = 5439
    db = 'dev'

    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{str(port)}/{db}')
    
    return engine
