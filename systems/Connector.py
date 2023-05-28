from pymongo import MongoClient
from sqlalchemy import create_engine
import os
import certifi


#open_machine = "mongodb+srv://nelson:gI5xU2OzRDHJcrsh@cluster0.oogfs.mongodb.net/?retryWrites=true&w=majority"
open_machine = "mongodb+srv://joe:R17meEbZWC3ZEcHe@cluster0.oogfs.mongodb.net/?retryWrites=true&w=majority"
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
        #mongo_cre2 = os.getenv('MONGO_ANALYTICS_CONN_STRING')
        mongo_cre2 = os.getenv('MONGO_ANALYTICS_CONN_STRINGx', open_machine)
        #__mongo = MongoClient(mongo_cre)
        __mongo = MongoClient(mongo_cre2, tlsCAFile=certifi.where())
       

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
    host = os.getenv('DB_HOSTx', '')
    user = os.getenv('DB_USERx', '')
    password = os.getenv('DB_PASSx', '')
    port = 5432
    db = 'sample_analytics'

    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{str(port)}/{db}')
    
    return engine


# import mietl

# source_schema = ['id', 'name', 'address', 'dob']
# destination_schema = ['id', 'name', 'address', 'age']

# source = mietl.Source(*kwargs) #NoSQL source
# destination = mietl.Destination(*kwargs)
# extraction_engine = mietl.Extraction(source_schema, destination_schema)







