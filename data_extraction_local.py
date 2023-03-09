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
from schema import REDHSHIFT_TABLE_COLUMNS
import warnings
from loader import redshift_s3_write

environment = os.getenv('ENVIRONMENT')



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



    #Processing time : This records time take to precesss each table. operations here include data cleaning, data type
    processed_dict = {}
    if data_dict:
        for k, v in data_dict.items():
            
            if k == 'trips':
                dropoff = pd.json_normalize(v, record_path=['dropOff'], meta=['tripId', '_id'], meta_prefix='v',errors='ignore', sep='_')
                dropoff.columns = dropoff.columns.str.lower()
                #trip_history = pd.json_normalize(v, record_path=['history'], meta=['tripId'], errors='ignore', sep='_')
                trips = pd.json_normalize(v, sep='_')
                # print('*' * 20)
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
                
            elif k == 'invoices':


                ########################################## Invoice Freight Request ##########################################
                """
                    these are needed keys/columns that are objects
                    NOTE: Some of the keys whose values are object are not needed that is why we need to specify the needed ones
                    in a list 
                """
                invoice_dict_columns = [i.lower() for i in [
                            "confirmedPaidBy",
                            "confirmedSentBy",
                            "createdBy",
                            "publishedBy",
                            "beneficiaryBank",
                        ]]

                #variable v here is a json object of the extracted invoice data
                invoice_df = pd.json_normalize(v)
                #holds the data (each element is a dictionary) for the invoice freight request
                freighRequest = []
                """
                    this loop through the trip column of the freight request data
                    and enumerate was used because the index is needed to get the value
                    in another column (invoice_id)
                """
                for df_index,field_value in enumerate(invoice_df["trips"]):
                    for element in field_value:
                        """
                            invoices with trips object that has freightLinked as true
                            are freight request invoices
                        """
                        if "freightLinked" in element.keys():
                            if element["freightLinked"] == True:
                                data = {}
                                freight = element["freight"]
                                data["freightRequestId"] = freight["freightRequestId"]
                                data["freightRequestReadId"] = freight["freightRequestReadId"]
                                data["freightGtvCurrency"] = freight["freightGtvCurrency"]
                                data["freightInvoiceAmount"] = element["freightInvoiceAmount"]
                                data["freightInvoiceAmountInInvoiceCurrency"] = element[
                                    "freightInvoiceAmountInInvoiceCurrency"
                                ]
                                data["freightInvoiceAmountInTripCurrency"] = element[
                                    "freightInvoiceAmountInTripCurrency"
                                ]
                                data["invoiceId"] = invoice_df.iloc[df_index]["invoiceId"]

                                freighRequest.append(data)
                
                #convert the list to a dataframe
                freighRequest_df = pd.DataFrame(freighRequest)
                #convert the column names to lowercase because that is the case used on redshift
                freighRequest_df.columns = freighRequest_df.columns.str.lower()
                #append the dataframe to the processed dictionary
                processed_dict['invoice_freight_requests'] = freighRequest_df
                #########################################################################################################


                ######################################## Base Invoice ###################################################

                #loop through the invoices columns
                for column in invoice_df.columns:
                    """
                        A flag is used it if a column name is generated by json_normalize, and/needs to be dropped
                    """
                    flag = False
                    #check if the column name is generated by json_normalize
                    #an column with dict values will be stretched with its keys as column names (parent_key.child_key)
                    #only ids have $ sign in their name and we dont want to drop them
                    if '.' in column and '$' not in column:
                        #checks if the parent key is in the dict_column list
                        if column.split('.')[0].lower() not in invoice_dict_columns:
                            flag = True
                        else:
                            flag= False
                    
                    if not flag:
                        """
                            A try and except is used as some column have null values in all fields, this can be avoided
                            by get not null values then check the size before trying to index the first element 
                        """
                        try:
                            #gets the not null fields , so as to peep into the first element to get the type
                            first_value = invoice_df[invoice_df[column].notnull()][column].iloc[0]
                            
                            #gets drop the column if the first element is a list
                            if isinstance(first_value,list):
                                # if 'externalreference' in i.lower():
                                #     ...
                                # else:
                                
                                invoice_df.drop(column,axis=1,inplace=True)
                        except:
                            pass
                    else:
                        
                        invoice_df.drop(column,axis=1,inplace=True)
                print('savong')
                
                #append the base invoice dataframe to the processed dictionary
                processed_data = invoice_df
                processed_data.columns = processed_data.columns.str.lower()
                processed_dict[k] = processed_data
                #########################################################################################################
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

            elif tableName == "trips":
                table['deliverydate'] = pd.to_datetime(table['deliverydate'])
            
            elif tableName == 'lossrecoveries_assigned':
                table['actiontime'] = table[table['actiontime'].notnull()]['actiontime'].apply(lambda x:date_convert(x))

            elif tableName == 'lossrecoveries_detached':
                table['actiontime'] = table[table['actiontime'].notnull()]['actiontime'].apply(lambda x:date_convert(x))

            try:
                print(f'+++++ {tableName} +++++')
                redshift_s3_write(tableName, table)
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