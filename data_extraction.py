import pandas as pd
import numpy as np
import os
import json
import csv
from bson import Timestamp, ObjectId
from datetime import datetime
import pymongo
import time
from systems import Connector
from systems.preprocess_func import date_convert
from systems.util import get_last_run, append_last_run, timestamp_to_bson_ts, send_mail
from schema import REDHSHIFT_TABLE_COLUMNS
import warnings
from loader import redshift_s3_write

environment = os.getenv('ENVIRONMENT')


# class OplogExtraction():
#     def __init__(self) -> None:
#         self.oplog_con = Connector.MongoOplog()
#         mongo_client = Connector.MongoConn()
#         self.mongo_con = mongo_client.kbanalytics

def oplog_extraction():
    """
    Create connection and poll mongo oplog
    """
    oplog_con = Connector.MongoOplog()
    mongo_client = Connector.MongoConn()
    mongo_con = mongo_client.kbanalytics

    # Extract bson timestamp from last run in either mongo or local machine
    ts = timestamp_to_bson_ts()
    # kindly ignore the long code on line 34 till we find a fix. 
    cursor = oplog_con.find({'ts': {'$gt': ts},
                            'ns' :{'$regex' : '^kbanalytics.fleets|^kbanalytics.inflowtransactions|^kbanalytics.trips|^kbanalytics.transactions|^kbanalytics.triphistories|^kbanalytics.customerconfigs|^kbanalytics.kbcare_productrequests|^kbanalytics.kbcare_vendorpayments|^kbanalytics.ksafe_incidents|^kbanalytics.incidence_prediction|^kbanalytics.wallets|^kbanalytics.customers|^kbanalytics.partnerconfigs|^kbanalytics.products|^kbanalytics.vouchers|^kbanalytics.payfastacustomers|^kbanalytics.vendor_products|\
                                               ^kbanalytics.paymentchannels|^kbanalytics.payfastapaymentrequests|^kbanalytics.requests|^kbanalytics.beneficiaries|^kbanalytics.freightrequests|^kbanalytics.truck_trackers|^kbanalytics.ksafe_allocations'}},
                            cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                            oplog_replay=True)

    data_dict = {}
    data_dict_update = {}

    # Abstract 'all_collection' so we can add or remove tables on the fly
    all_collection = ['fleets', 'inflowtransactions', 'triphistories',
                      'transactions', 'customerconfigs', 'kbcare_productrequests', 'kbcare_vendorpayments', 'ksafe_incidents',
                      'incidence_prediction', 'wallets', 'customers', 'vendor_products', 'payfastacustomers', 'payfastatransactions', 
                      'vouchers','products', 'partnerconfigs', 'requests', 'beneficiaries', 'paymentchannels', 'payfastapaymentrequests',
                      'freightrequests', 'messages', 'truck_trackers', 'ksafe_allocations','trips'
                    ]
    #start_time = time.time()

    ######################## Context ##########################
    # We poll oplog until all chnages have been exhausted
    # We are aware that should there be new changes/updates as the poll runs, it will keep cursor alive
    # However, this will run at midnight and chances of updates around that time is almost zero
    #########################################################################################################

    start = datetime.now()
    print(f"We are in {environment} environment.")
    print(f"Last run time we working with:\n{datetime.fromtimestamp(get_last_run())}")

    #cursor.live remians True until it is exhausted and it goes False
    print(f"Commencing Oplog polling at: {start}\n ....")
    while cursor.alive:
        for doc in cursor:
            if doc['op'] == 'u':
            #    print(f"Collection_name: {doc['ns']}, Object_id: {doc['o2']['_id']}")
               collection_name = doc['ns'].split('.')[-1]
            #    print(f'UPDATE!! : Collection name ===> {collection_name}')
            #    print(f"{'*' * 10}\n")
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
                        
            if (datetime.now() - start).total_seconds() > (60*60)*2:
                break
        break

    """
        *Problem 1 : Update operations might have repeated ids which contain same document.
        *Problem 2 : Ids might be present both in insert dict and update dict which will created duplicate doc 
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
    extract_time =  stop_date - start
    len_tables = len(data_dict.keys())
    changed_tables = '|'.join(data_dict.keys())
    

    

    #print(f"Final updated dict is :====> {data_dict_update}")

    """
    json normalize each table, convert all columns names to lower case and store back to a dictionary
    names processed_dict with thier table names as respective keys
    """
    process_start_time = datetime.now()
    processed_dict = {}
    if data_dict:
        for k, v in data_dict.items():
            if k == 'trips':
                dropoff = pd.json_normalize(v, record_path=['dropOff'], meta=['tripId', '_id'], meta_prefix='v',errors='ignore', sep='_')
                dropoff.columns = dropoff.columns.str.lower()
                #trip_history = pd.json_normalize(v, record_path=['history'], meta=['tripId'], errors='ignore', sep='_')
                trips = pd.json_normalize(v, sep='_')
                if 'LoadedDate' in trips.columns:
                    trips.drop(columns='LoadedDate', inplace=True)
                trips.columns = trips.columns.str.lower()
                trips.loc[trips['isdedicated'] == False, 'isdedicated']= 'ondemand'
                trips.loc[trips['isdedicated'] == True, 'isdedicated']= 'dt'
                #processed_dict['trip_history'] = trip_history
                processed_dict['dropoff'] = dropoff
                processed_dict[k] = trips
                verificationImages_list = []
                for i in v:
                    _id = i['_id']
                    tripId = i['tripId']
                    try:
                        verificationImages = i['verificationImages']
                    except:
                        verificationImages = []
                    if verificationImages:
                        if isinstance(verificationImages,dict):
                            i['verificationImages']['tripreadid'] = tripId
                            i['verificationImages']['tripid'] = _id
                            i['verificationImages']['_id'] = np.NaN
                            i['verificationImages']['attachments'] = i['verificationImages']['0'].pop('attachments')
                            i['verificationImages']['uploadedBy'] = i['verificationImages']['0'].pop('uploadedBy')
                            i['verificationImages']['date'] = i['verificationImages']['0'].pop('date')
                            verificationImages_list.append(i['verificationImages'])
                        else:
                            for j in range(len(verificationImages)):
                                try:
                                    if i['verificationImages'][j]['_id']:
                                        pass
                                except:
                                    i['verificationImages'][j]['_id'] =  np.NaN
                                i['verificationImages'][j]['tripreadid'] = tripId
                                i['verificationImages'][j]['tripid'] = _id
                                verificationImages_list.append(i['verificationImages'][j])

                """
                If doc from verification image is a dict, it creates '0' with an empty dictionary
                the code below delets the '0' 
                """
                try:
                    del verificationImages_list[0]['0']
                except KeyError as e:
                    pass
                verification_images = pd.json_normalize(verificationImages_list, sep='_')
                verification_images_attachment = pd.json_normalize(
                                verificationImages_list, record_path=['attachments'], meta_prefix= 'v',
                                meta=['tripid', 'tripreadid', '_id'], sep='_'
                )
                
                verification_images_attachment.rename(columns={
                                                'vtripid': 'tripid',
                                                'vtripreadid': 'tripreadid',
                                                'v_id':'trip'}, inplace=True)

                dropoff.rename(columns={
                                    'vtripid': 'tripreadid',
                                    'v_id': 'tripid'
                                        }, inplace=True)
                verification_images.columns = verification_images.columns.str.lower()
                verification_images_attachment.columns = verification_images_attachment.columns.str.lower()
                processed_dict['verification_images'] = verification_images
                processed_dict['verification_images_attachment'] = verification_images_attachment

            elif k == 'requests':
                requests = pd.json_normalize(v, sep='_')
                requests.rename(columns={
                    'requestId': 'readablerequestid',
                    '_id' : 'requestid'  
                }, inplace=True)
                requests.columns = requests.columns.str.lower()
                processed_dict['requests'] = requests

            elif k == 'lossrecoveries':
                loss_assigned = []
                loss_detached = []
                for i in v:
                    recovery_id = i['_id']
                    recoveryid = i["recoveryId"]
                    initalamont = i["initialAmount"]
                    recoverytype =  i["recoveryType"]
                    recoveryname =   i["recoveryName"]
                    status =    i["status"]
                    systemtype = i["systemType"]
                    currency =    i["currency"]
                    createddate =   i["createdDate"]
                    totalamountleft = i["totalAmountLeft"]
                    isvoid =   i["isVoid"]
                    if not i['assigned']:
                        pass
                    else:
                        for k in i['assigned']:
                            k['recovery_id'] = recovery_id
                            k["recoveryId"] = recoveryid
                            k["initialAmount"] = initalamont
                            k["recoveryType"] = recoverytype
                            k["recoveryName"] = recoveryname
                            k["status"] = status
                            k['systemType'] = systemtype
                            k['currency'] = currency
                            k['createdDate'] = createddate
                            k['totalAmountLeft'] = totalamountleft
                            k['isVoid'] = isvoid
                            loss_assigned.append(k)
                    
                    if not i['detachedAssignments']:
                        pass
                    else:
                        for k in i['detachedAssignments']:
                            k['recoveryId'] = recoveryid
                            k['recovery_id'] = recovery_id
                            k['currency'] = currency
                            loss_detached.append(k)
                loss_assigned_tab = pd.json_normalize(loss_assigned, sep='_')
                loss_assigned_tab.columns = loss_assigned_tab.columns.str.lower()
                loss_detached_tab = pd.json_normalize(loss_detached, sep='_')
                loss_detached_tab.columns = loss_detached_tab.columns.str.lower()
                processed_dict['lossrecoveries_assigned'] = loss_assigned_tab
                processed_dict['lossrecoveries_detached'] = loss_detached_tab
                
            else:
                processed_data = pd.json_normalize(v, sep = '_')
                processed_data.columns = processed_data.columns.str.lower()
                processed_dict[k] = processed_data


    """
    comapre the schema of each table from data dict to thier respective redshift schema,
    If a column present in trespective redshift schema doesnt exist in table from processed dict,
    create this column and fill with NaN. And finally remove columns that arent needed 
    """
    
    for key in processed_dict.keys():
        if key in REDHSHIFT_TABLE_COLUMNS.keys():
            for col in REDHSHIFT_TABLE_COLUMNS[key]:
                if col not in processed_dict[key].columns:
                    processed_dict[key][col] = np.nan
        
        processed_dict[key] = processed_dict[key][REDHSHIFT_TABLE_COLUMNS[key]]
        #processed_dict[key].to_csv(key + '.csv', index=False)
        
    processs_time = process_start_time - datetime.now()   
    return True, processed_dict, extract_time, stop_date, stop_time, processs_time

def handler(event, context):
    """
    Deal.
    """
    stats, result, extraction_time, stop_date, stop_time, processs_time = oplog_extraction()
    len_tables = len(result.keys())
    changed_tables = '|'.join(f"{k} {len(v)}" for k, v in result.items())
    if stats:
        start_load = datetime.now()
        print("Starting S3 and Redshift operations....")
        #write to S3 and Redshift
        for tableName, table in result.items():
            #convert bson
            for col in table.columns:
                temp_table = table[table[col].notnull()][col]
                if temp_table.empty:
                    pass
                else:
                    if type(temp_table.iloc[0]) == ObjectId:
                        table[col] = [str(line) for line in table[col]]


            if tableName == 'fleets':     
                #and another for fleets in particular
                #########################3 Give us a more elegant handling here ################################
                # Lets peep the first item of every col asking the type they are. If they are bson, convert it to str ===> fixed in LINE 194
                #########################################################################################################
                table['pooloutdate'] = table[table['pooloutdate'].notnull()]['pooloutdate'].apply(
                lambda x: pd.to_datetime(int(float(x)) / 1000, unit='s') 
                if (len(str(x)) == 15 and isinstance(x, float))
                or (len(str(x)) == 13 and isinstance(x, str))
                else pd.to_datetime(x)
                )
                table['pooloutdate'] = pd.to_datetime(table['pooloutdate'])
                incorrect_dates = ['0', '0000-00-00 00:00:00', '', 'NULL', np.nan]
                table.loc[table['pooldate'].isin(incorrect_dates), 'pooldate'] = pd.NaT
                table.loc[:, 'pooldate'] = pd.to_datetime(table['pooldate'], errors='coerce')

            elif tableName == 'dropoff':
                table['deliverydate'] = table[table['deliverydate'].notnull()]['deliverydate'].apply(lambda x: date_convert(x))

            elif tableName == 'lossrecoveries_assigned':
                table['actiontime'] = table[table['actiontime'].notnull()]['actiontime'].apply(lambda x:date_convert(x))

            elif tableName == "trips":
                table['deliverydate'] = pd.to_datetime(table['deliverydate'])

            elif tableName == 'lossrecoveries_detached':
                table['actiontime'] = table[table['actiontime'].notnull()]['actiontime'].apply(lambda x:date_convert(x))


            try:
                redshift_s3_write(tableName, table)
            except Exception as e:
                send_mail(message=f"Error Writing to Redshift", subject='Failure!')
    end_load = datetime.now()
    total_load_time = end_load - start_load

    """
    call append_last_run function to records runs
    NOTE!! append_last_run with parameters inserts meta-datato mongo , while append_last_run without
          parameters inserts to local machine date.csv.
    """ 

    try:
        if environment != 'highway':
            append_last_run()
        else:
            #Append meta data info on run
            append_last_run(len_table=len_tables, changed_tables=changed_tables,
                            current_date= stop_date, current_time=stop_time,
                            extract_time=extraction_time.total_seconds(), 
                            processsing_time=processs_time,
                            load_time = total_load_time.total_seconds(), env=environment)
    except Exception as e:
        print(f"Error writing oplog-works run details: {str(e)}")

    
    #Report a successful run
    send_mail(message=f"Successfully ran batch load pipeline.\n\n\
                        Extraction Time ==> {extraction_time}\n\n\
                        S3 and Redshift load time ==> {total_load_time}\n\n\
                        process_time => {processs_time}\n\n\n\
                        Changed Tables ==> {changed_tables}\n\n- Data Engineering.")

    print('Done')