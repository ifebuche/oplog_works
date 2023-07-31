from datetime import datetime as dt
import os
import random
import awswrangler as wr
from .systems.util import update_loader_status

environment = os.getenv('ENVIRONMENT')





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


    def __init__(self,mongo_conn):
        self.mongo_conn = mongo_conn

    @staticmethod
    def s3_upload(data, bucket_name:str, prefix:str, table_name, file_format='parquet'):
        
        """
        prefix: if prefix more than one, include a "/"
        """
        year = dt.now().year
        month = dt.now().strftime("%B")
        day = dt.now().day
        location = f"s3://{bucket_name}/{prefix}/{year}/{month}/{day}/{table_name}_{dt.now().time()}"

        # Unsure about the forloop, Need Pascal's Input.
        try:
            wr.s3.to_parquet(data, path=location)
        except Exception as e:
            print(f"Error writing to S3: => {e}")

    # def redshift_upload():

    # def snowflake():

    @staticmethod
    def insert_update_record(engine, df, targetTable, mongo_conn, pk='_id'):
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
            update_loader_status(mongo_conn)
            return True, "Transaction successful!"
        except Exception as e:
            msg = f"Problem writing to RedshiftConn: => {e}"
            # print(msg)
            # if environment == 'highway':
            #     send_mail(msg, subject='Error')
            # else:
            #     pass
            #Drop the temp table. Since the transaction failed
            with engine.begin() as conne:
                conne.execute(drop)
            # capture_exception(e)
            return False, str(e)