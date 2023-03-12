import pandas as pd
import numpy as np
import os
import json
import csv
from bson.errors import InvalidBSON
#from pymongo.errors import InvalidBSON
from bson import Timestamp, ObjectId
from datetime import datetime
import pymongo
import time
from systems import Connector
from systems.preprocess_func import date_convert
from systems.util import append_last_run, timestamp_to_bson_ts, send_mail
from schema import DESTINATION_TABLE_COLUMNS
import warnings
from loader import Loader



environment = os.getenv('ENVIRONMENT')


source_con = mi_etl.Connector.Source(pormm, kdvsd)
extracted_doc = mi_etl.data_Extraction.oplog_Extraction(collection_names, source_con)


def oplog_extraction():
    #Create connection and poll mongo oplog 
    oplog_con = Connector.MongoOplog()
    mongo_client = Connector.MongoConn()
    mongo_con = mongo_client.kbanalytics

    # Extract bson timestamp from last run in either mongo or local machine
    ts = timestamp_to_bson_ts()
    
    #Extraction time -> This includes time from pulling from oplog and usind _ids to extract full document for update operations
    start_extract_time = datetime.now()

    cursor = oplog_con.find({'ts': {'$gt': ts},
                        'ns' :{'$regex' : '^sample_analytics.customers|^sample_analytics.accounts|^sample_analytics.transactions'}},
                        cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                        oplog_replay=True)


    data_dict = {}
    data_dict_update = {}

    # Abstract 'all_collection' so we can add or remove tables on the fly



    all_collection = ['customers', 'accounts', 'transactions']


    ######################## Context ##########################
    # We poll oplog until all chnages have been exhausted
    # We are aware that should there be new changes/updates as the poll runs, it will keep cursor alive
    # However, this will run at midnight and chances of updates around that time is almost zero
    #########################################################################################################


    start = datetime.now()
    #cursor.live remians True until it is exhausted and it goes False
    Last_Doc = {}

    while cursor.alive:
       
        for doc in cursor:
            Last_Doc["doc"] = doc
            ts_var = doc['ts']
            Last_Doc["ts"] = ts_var
            if doc['op'] == 'u':
                
                print(f"Collection_name: {doc['ns']}, Object_id: {doc['o2']['_id']}")
                collection_name = doc['ns'].split('.')[-1]
                print(f'UPDATE!! : Collection name ===> {collection_name}')
                print(f"{'*' * 10}\n")
                if collection_name in all_collection:
                
                    if collection_name in data_dict_update.keys():
                        data_dict_update[collection_name].append(doc['o2']['_id'])

                    else:
                        data_dict_update[collection_name] = [doc['o2']['_id']]
                    

            else:
                
                df_dict = doc.get('o')
                collection_name = doc['ns'].split('.')[-1]
                print(f"INSERT :Collection name ==> {collection_name}")
                print(f"{'*' * 10}\n")
                if collection_name in all_collection:
                    if collection_name in data_dict.keys():
                        data_dict[collection_name].append(df_dict)
                    else:
                        data_dict[collection_name] = [df_dict]        
            
        if (datetime.now() - start).total_seconds() > (60*60)*10:
                break
        break

    """
        *Problem 1 : Update operations might have repeated ids which contain same document.
        *Problem 2 : Ids might be present both in insert dict and update dict which will create duplicate doc 
        *First => we use set to make these ids unique
        *Second => we remove doc in insert dict of ids that are present in update dict
    """

    # This fixes problem 1
    data_dict_update = {key: list(set(value)) for key, value in data_dict_update.items()}
    
    # This fixes problem 2
    for k, v in data_dict_update.items():
        final_inserts_list = []
        insert = data_dict.get(k)
        if insert:
            for doc in insert:
                if not doc['_id'] in v:
                    final_inserts_list.append(doc)
            data_dict[k] = final_inserts_list
        else:
            continue


    """
    Extract entire document from data_dict_update using ids and append them to data_dict based
    on thier respective keys 
    """
    for key, value in data_dict_update.items():
        collection_name = key 
        df = mongo_con[collection_name].find({'_id': {"$in" : value}})
        for d in df:
            if collection_name in all_collection:
                if collection_name in data_dict.keys():
                    data_dict[collection_name].append(d)
                                
                else:
                    data_dict[collection_name] = [d]

    # exract metadata to store in batch_pipe_run_time mongo collection
    stop_date = datetime.now()
    stop_time = time.time()
    extract_time = datetime.now() - start_extract_time
    

    """
    call append_last_run function to records runs
    NOTE!! append_last_run with parameters inserts meta-datato mongo , while append_last_run without
          parameters inserts to local machine date.csv.
    """ 

    #print(f"Final updated dict is :====> {data_dict_update}")

    """
    json normalize each table, convert all columns names to lower case and store back to a dictionary
    names processed_dict with thier table names as respective keys
    """


    #############################################################################################
    ####################### Split out this part as a different job: transformation ##############
    #############################################################################################
   
    #Processing time : This records time take to precesss each table. operations here include data cleaning, data type 
    processed_dict = {}
    if data_dict:
        for k, v in data_dict.items():

            if k == 'customers':
                customers_collection_df = pd.json_normalize(v)

                customers_collection_df.columns = list(map(lambda column_name: column_name.lower(), customers_collection_df))

                processed_dict[k] = customers_collection_df
            
    """
    comapre the schema of each table from data dict to thier respective redshift schema,
    If a column present in trespective redshift schema doesnt exist in table from processed dict,
    create this column and fill with NaN. And finally remove columns that arent needed 
    """
    
    ##############################################################################################
    ####################### Consider splitting out schema resolution #############################
    ##############################################################################################
    for key in processed_dict.keys():
        if key in DESTINATION_TABLE_COLUMNS.keys():
            for col in DESTINATION_TABLE_COLUMNS[key]:
                if col not in processed_dict[key].columns:
                    processed_dict[key][col] = np.nan
        
        processed_dict[key] = processed_dict[key][DESTINATION_TABLE_COLUMNS[key]]
        processed_dict[key].to_csv(key + '.csv', index=False)
        #Convert bson ObjectIdto string
        # print('Before conversion of ObjectId', '\n', [type(line) for line in processed_dict[key]['_id']])
        # processed_dict[key]['_id'] = [str(line) for line in processed_dict[key]['_id']]
        # print('After conversion of ObjectId', '\n', [type(line) for line in processed_dict[key]['_id']])

    return True, processed_dict, extract_time, stop_date, stop_time

def handler(event = None, context = None):
    """
    Deal.
    """
    stats, result, extraction_time, stop_date, stop_time = oplog_extraction()
    len_tables = len(result.keys())
    changed_tables = '|'.join(f"{k} {len(v)}" for k, v in result.items())
    if stats:
        start_load = datetime.now()
        print("Starting S3 and Redshift operations....")
        #write to S3 and Redshift
        for tableName, table in result.items():
            #convert bson
            #Some how there are two loadeddate(loadeddate, loadeddate.1) and this was causing the pipeline to break
            #even when they mean the same thing
            # breakpoint()
            
            for col in table.columns:
                temp_table = table[table[col].notnull()][col]
                if temp_table.empty:
                    pass
                else:
                    if type(temp_table.iloc[0]) == ObjectId:
                        table[col] = [str(line) for line in table[col]]

            try:
                print(f'+++++ {tableName} +++++')
                Loader.warehouse_cdc_write(tableName, table)
            except Exception as e:
                send_mail(message=f"Error Writing to Redshift", subject='Failure!')


    end_load = datetime.now()
    total_load_time = end_load - start_load

    try:
        if environment != 'highway':
            append_last_run()
        else:
            append_last_run(len_table=len_tables, changed_tables=changed_tables,
                            current_date= stop_date, current_time=stop_time,
                            extract_time=extraction_time.total_seconds(), 
                            load_time = total_load_time.total_seconds(), env=environment)
    except Exception as e:
        print(f"Error writing oplog-works run details: {str(e)}")

    
    #Report a successful run
    # table_and_lenght = changed_tables.split('|')
    # nl = '\n'
    # send_mail(message=f"Successfully ran batch load pipeline.\n\n\
    #         Extraction Time ==> {extraction_time}\n\
    #     S3 and Redshift load time ==> {total_load_time}\n\n\
    #     Chnaged Tables ==> {nl.join(table_and_lenght)}\n\n- Data Engineering.")

    print('Done')


if __name__ == "__main__":
    print("+++++++++++++ RUNNING LOCALLY +++++++++++++")
    handler()