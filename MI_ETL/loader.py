from datetime import datetime as dt
import os
import random
import awswrangler as wr
from .util import update_loader_status
from connector import Destination

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
    def __init__(self,mongo_conn,data):
        self.mongo_conn = mongo_conn
        self.data = data
        #initialize a class Loader, 
    @staticmethod
    def s3_upload(data, bucket_name:str, table_name, prefix:str = None):
        """Upload dataframe to s3

        Args:
            data (dataframe): 
            bucket_name (str): s3 bucket name
            prefix (str): parent folder name
            table_name (str): name of the table used as part of the filename
            file_format (str, optional): filetye. Defaults to 'parquet'.
        """
        year = dt.now().year
        month = dt.now().strftime("%B")
        day = dt.now().day
        if prefix:
            location = f"s3://{bucket_name}/{prefix}/{year}/{month}/{day}/{table_name}_{dt.now().time()}"
        else:
            location = f"s3://{bucket_name}/{year}/{month}/{day}/{table_name}_{dt.now().time()}"
        
        # Unsure about the forloop, Need Pascal's Input.
        try:
            wr.s3.to_parquet(data, path=location)
        except Exception as e:
            
            result = f"Error writing to S3: => {e}"
            return False, result
        
        return True, 'Success'

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
            # update_loader_status(mongo_conn)
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
        

    def load_datalake(self, *kwargs):
        if 'bucket_name' not in kwargs.keys():
                raise OplogWorksError("s3_upload","'bucket name' missing")

        s3_params = {
                           'bucket_name': kwargs['bucket_name'] 
                        }
        if 'prefix' in kwargs.keys():
            s3_params['prefix'] = kwargs['prefix']
        failed_tables = []
        successful_tables = []
        counter = 0
        for k, v in self.data.items():
            #why we are ignoring cmd and metadata (remove unwanted collection/tables)
            if k not in ['$cmd', 'metadata']:
                s3_params['data'] = v
                s3_params['table_name'] = k
                
                ok, message = self.s3_upload(s3_params)
                
                if not ok:
                    print(f"table '{k}' failed - {message}")
                    failed_tables.append(k)
                else:
                    successful_tables.append(k)
                    print(f"table '{k}' succesful - {message}")

                counter +=1
                #     raise OplogWorksError("s3_upload",f"File not written to s3, {data}")

            outcome = {
                'successful_tables' : successful_tables,
                'failed_tables': failed_tables,
                'message': f'{len(successful_tables)}/{counter} tables done.',
                'description': 'S3 load job result'
            }

        return outcome

    def load_warehouse(self, *kwargs):
        required_params = ['user','password','host','db','port']
        for item in required_params:
            #ensure the required keys is passed and it is not an empty string
            if item not in kwargs.keys() and kwargs.get(item):
                raise OplogWorksError("redshift_upload",f"'{item}' cannot be empty")
        
        redshift_params = {
                'user': kwargs['user'],
                'password': kwargs['password'],
                'host': kwargs['host'],
                'port': kwargs['port'],
                'db': kwargs['db']
        }
        engine = Destination.redshift(redshift_params)
        failed_tables = []
        successful_tables = []
        counter = 0
        #  = create_engine(f'postgresql://root:root@172.18.0.2:5432/postgres') #initialize enine
        for collection in self.data.keys():
            if collection != 'metadata':
                ok , message = self.insert_update_record(engine=engine, df = self.data[collection], targetTable=collection, pk='_id')
                # Loader.insert_update_record
                if not ok:
                    print(f"table '{collection}' failed - {message}")
                    failed_tables.append(collection)
                else:
                    successful_tables.append(collection)
                    print(f"table '{collection}' succesful - {message}")

                counter +=1

        outcome = {
                    'successful_tables' : successful_tables,
                    'failed_tables': failed_tables,
                    'message': f'{len(successful_tables)}/{counter} tables done.',
                    'description': 'Redshift load job result'
                    }

        Loader.update_loader_run(mongo_conn=self.mongo_conn)
        return outcome


    def run(self, datalake=None, warehouse=None, *kwargs):
        #docs should clear on what kwargs want to achieve
        if datalake and not warehouse:
            
            return self.load_datalake()
            
            #write metadata
        elif datalake and warehouse:
            outcome1 = self.load_datalake()
            outcome2 = self.load_warehouse()

            return outcome1, outcome2
            #write metadata
        elif not datalake and warehouse:
            return self.load_warehouse()
            #write metadata

        #validate that all the credentials were supplied for s3 
        #prefix should be optional
        #add custom errors
        #move outside s3 to init
        #alert and lodinh should both alert
        #function for result metadata

    