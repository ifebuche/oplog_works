from datetime import datetime as dt
import os
import random
import awswrangler as wr
from systems.util import send_mail
from systems.Connector import WarehouseConn

engine = WarehouseConn()
environment = os.getenv('ENVIRONMENT')


year = dt.now().year
month = dt.now().strftime("%B")
day = dt.now().day


class OplogWorksError(Exception):
    def __init__(self, func_name, issue):
        self.func_name = func_name
        self.issue = issue
    def __str__(self):
        return f"Error at {self.func_name}: \nIssue => {self.issue}"

class Loader:
    """
    Define Lake and Warehouse options
    """
    # def __init__(self, service):
    #     self.resource = None
    #     if self.service.lower() == 'aws':
    #         self.lake = 's3'
    #         self.warehouse = 'postgres'
    #     if self.service.lower() == 'azure':
    #         self.resource = 'blobstorage'
    #         self.warehouse = 'synapse'



    def s3_upload(self, tableName, data, year=year, month=month, day=day):
        """
        Put parquet objects into s3 bucket, organising them by table name and production date.
        
        data: df - dataframe to be written
        """
        if environment != 'highway':
            loc = f's3://mi-etl/oplog_works_stage/{tableName}/{year}/{month}/{day}/{tableName}_{dt.now().time()}.parquet'
        else:
            loc = f's3://mi-etl/oplog_works/{tableName}/{year}/{month}/{day}/{tableName}_{dt.now().time()}.parquet'
        try:
            wr.s3.to_parquet(df=data, path=loc)
            print("Wrote to S3 successfully!")
        except Exception as e:
            print(f"Error writing to S3: => {e}")
            # capture_exception(e)



    def insert_update_record(self, engine, df, targetTable, pk):
        """
        Update redhsift table via transaction.

        engine: sqlalchemy engine
        df: processed table from the stream.
        targetTable: table name on RedshiftConn
        pk: key to join update on across both tables
        """
        print("Commencing RedshiftConn write...")

        random_number = random.randint(10, 10000)
        temp = targetTable +'_temp' + str(random_number) #Name for our temporary table. An appendage of 'temp123' to main table name. Using same value could mean that at high velocity, temp table is destroyed while in use with the drop after a write

        #Queries to run.

        print(f"Incoming table is {targetTable}")
        
        create = f"create table if not exists {temp} (like {targetTable});"
        drop = f"drop table {temp}"
        transact = f"""
                    begin transaction;

                    delete from {targetTable} using {temp} where {targetTable}.{pk} = {temp}.{pk};

                    insert into {targetTable} select * from {temp};

                    drop table {temp};

                    end transaction;
                """
        
        #Commence update attempt
        try:
            print(f"Creating temp table {temp}")
            create_response, transact_response = 0, 0
            with engine.begin() as conne:
                create_res = conne.execute(create)
                create_response += create_res.rowcount
                if not create_response:
                    msg = (f"Failure creating {temp} table")
                    print(msg)
                    raise OplogWorksError("insert_update_record()", f"\nError message => {msg}")
                    # capture_exception()
                    return False, msg
            try:
                df.to_sql(temp, engine, index=False, if_exists='append')
            except Exception as e:
                with engine.begin() as conne:
                    conne.execute(drop)
                    raise OplogWorksError("insert_update_record()", f"\nError message => {str(e)}")
                    # # capture_exception()
                    # return False, f"We could not load the temp table.\nErro: => {e}"

            with engine.begin() as conne:
                transact_res = conne.execute(transact)
                if transact_res:
                    print("Transaction Successful")
                    transact_response += transact_res.rowcount

            return True, "Transaction successful!"
        except Exception as e:
            msg = f"Problem writing to RedshiftConn: => {e}"
            print(msg)
            if environment == 'highway':
                send_mail(msg, subject='Error')
            else:
                pass
            #Drop the temp table. Since the transaction failed
            with engine.begin() as conne:
                conne.execute(drop)
            # capture_exception(e)
            return False, str(e)
        
        
    def warehouse_cdc_write(self, tableName, table):
        """
        Write a CDC copy to a datastore/datalake and onboard data into warehouse

        - Try dumping on S3, write status to errors dict
        - Try writing to RedshiftConn, write status in result
        """
        
        #Write to s3
        try:
            #Add current date as column
            table_2_s3 = table.copy()
            table_2_s3['write_date'] =  dt.now()
            self.s3_upload(tableName, table_2_s3)
        except Exception as e:
            print(f"Error dumping to S3: => {e}")
            # capture_exception(e)
        
        #write to RedshiftConn
        try:
            redshift_conn_inserted, msg = self.insert_update_record(engine, table, f"{tableName}", '_id')
            if redshift_conn_inserted:
                print("RS insert successful")
        except Exception as e:
            # capture_exception(e)
            msg = f"Error writing to RedshiftConn => {e}"
            print(msg)

    
    