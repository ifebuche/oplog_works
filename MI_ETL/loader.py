import random
from datetime import datetime as dt

import awswrangler as wr
import pandas as pd
from sqlalchemy import inspect, text

from .Connector import Destination
from .Error import OplogWorksError
from .systems.util import (schema_validation, update_loader_status, validate_kwargs)

class Loader:
    """
    A class for handling the loading of data into data lakes and warehouses.

    Provides functionalities to upload data to Amazon S3 and insert or update records
    in data warehouses. It uses AWS Wrangler for S3 operations and SQLAlchemy for 
    database interactions.

    Attributes:
        mongo_conn (MongoClient): MongoDB connection for updating loader status and retrieving timestamps.
        data (dict): Data to be loaded, with keys as table/collection names and values as data in pandas DataFrame format.
    """

    def __init__(self, mongo_conn, data: dict, datalake: bool, datawarehouse: bool, aws: dict):
        self.mongo_conn = mongo_conn
        self.data = data
        self.datalake = datalake
        self.warehouse = datawarehouse
        self.aws = aws


    @staticmethod
    def s3_upload(s3_params):  # data, bucket_name:str, table_name, prefix:str = None):
        """
        Uploads a DataFrame to an S3 bucket.

        Args:
            s3_params (dict): Parameters for the S3 upload including the DataFrame, 
                              bucket name, table name, prefix, and file format.

        Returns:
            tuple: A tuple containing a boolean indicating success or failure, 
                   and a message string describing the result of the operation.
        """
        validate_kwargs(s3_params, ["bucket_name", "table_name"], "s3_upload")

        data = s3_params.get("data", pd.DataFrame())
        bucket_name = s3_params.get("bucket_name", False)
        table_name = s3_params.get("table_name", False)
        prefix = s3_params.get("prefix", False)

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

        return True, "Success"

    @staticmethod
    def insert_update_record(engine, df, targetTable, pk="_id"):
        """
        Updates a table in a data warehouse using a transactional approach.

        This method creates a temporary table, performs deletion and insertion,
        and then drops the temporary table to update the target table.

        Args:
            engine (sqlalchemy.engine.Engine): The SQL database engine.
            df (pd.DataFrame): The DataFrame containing the data to be processed.
            targetTable (str): The name of the target table in the database.
            pk (str, optional): The primary key column name. Defaults to '_id'.

        Returns:
            tuple: A tuple containing a boolean indicating success or failure, 
                   a message, and a list of new columns added to the table.
        """
        print("Commencing RedshiftConn write...")

        random_number = random.randint(10, 10000)
        temp = (
            targetTable + "_temp" + str(random_number)
        )  # Name for our temporary table. An appendage of 'temp123' to main table name. Using same value could mean that at high velocity, temp table is destroyed while in use with the drop after a write

        #find another way to allow json data in columns to not break the insert
        # x = df.select_dtypes(include=["object"]).columns
        # for i in x:
        #     df[i] = list(map(lambda x: json.dumps(x), df[i]))
        columns_to_drop = None
        print(f"Incoming table is {targetTable}")

        #first database connection use by psycopg2, listen for OperationalError
        try:
            if not inspect(engine).has_table(targetTable):
            # if not engine.dialect.has_table(engine, targetTable):
                print(f"{targetTable} not found, creating...")
                df.to_sql(targetTable, engine, index=False, if_exists="append")
                print(f"{targetTable} created and loaded")
            else:
                # schema validation
                df, columns_to_drop = schema_validation(targetTable, engine, df)

                create = f"create table if not exists {temp} (like {targetTable});"
                drop = f"drop table {temp}"
                transact = f"""
                            begin transaction;

                            delete from {targetTable} using {temp} where {targetTable}.{pk} = {temp}.{pk};

                            insert into {targetTable} select * from {temp};

                            drop table {temp};

                            end transaction;
                        """.replace(
                    "None", "NULL"
                )

                # Commence update attempt
                try:
                    print(f"Creating temp table {temp}")
                    create_response, transact_response = 0, 0

                    with engine.begin() as conne:
                        create_res = conne.execute(text(create))
                        create_response += create_res.rowcount
                        if not create_response:
                            msg = f"Failure creating {temp} table"
                            print(msg)
                            raise OplogWorksError(
                                "insert_update_record()", f"\nError message => {msg}"
                            )
                            # capture_exception()
                            return False, msg
                    try:
                        df.to_sql(temp, engine, index=False, if_exists="append")
                    except Exception as e:
                        with engine.begin() as conne:
                            conne.execute(text(drop))
                            raise OplogWorksError(
                                "insert_update_record()", f"\nError message => {str(e)}"
                            )
                            # # capture_exception()
                            # return False, f"We could not load the temp table.\nErro: => {e}"

                    with engine.begin() as conne:
                        transact_res = conne.execute(text(transact))
                        if transact_res:
                            print("Transaction Successful")
                            transact_response += transact_res.rowcount
                    # update_loader_status(mongo_conn)
                    return True, "Transaction successful!" , columns_to_drop
                except Exception as e:
                    msg = f"Problem writing to RedshiftConn: => {e}"
                    with engine.begin() as conne:
                        conne.execute(text(drop))
                    # capture_exception(e)
                    return False, str(e) , columns_to_drop
            return True, "Transaction successful!", columns_to_drop
        except Exception as e:
            raise OplogWorksError('Loader.insert_update_record', str(e))

    def load_datalake(self, *args, **kwargs):
        """
        Loads data to an S3 data lake.

        This method iterates over the data attribute and uploads each table/collection 
        to an S3 bucket.

        Args:
            kwargs: Additional keyword arguments including the bucket name and optional prefix.

        Returns:
            dict: A summary of the loading operation, including lists of successfully 
                  and unsuccessfully loaded tables.
        """


        validate_kwargs(kwargs, ["bucket_name"], "load_datalake")

        s3_params = {"bucket_name": kwargs["bucket_name"]}
        if "prefix" in kwargs.keys():
            s3_params["prefix"] = kwargs["prefix"]
        failed_tables = []
        successful_tables = []
        counter = 0
        for k, v in self.data.items():
            # why we are ignoring cmd and metadata (remove unwanted collection/tables)
            if k not in ["$cmd", "metadata"]:
                s3_params["data"] = v
                s3_params["table_name"] = k
                # print(s3_params)
                ok, message = self.s3_upload(s3_params)

                if not ok:
                    print(f"table '{k}' failed - {message}")
                    failed_tables.append(k)
                else:
                    successful_tables.append(k)
                    print(f"table '{k}' succesful - {message}")

                counter += 1
                #     raise OplogWorksError("s3_upload",f"File not written to s3, {data}")

            outcome = {
                "successful_tables": successful_tables,
                "failed_tables": failed_tables,
                "message": f"{len(successful_tables)}/{counter} tables done.",
                "description": "S3 load job result",
            }

        return outcome

    def load_warehouse(self, **kwargs):
        """
        Loads data to a data warehouse.

        This method iterates over the data attribute and inserts or updates each 
        table/collection in the data warehouse.

        Args:
            kwargs: Additional keyword arguments for the warehouse connection.

        Returns:
            dict: A summary of the loading operation, including lists of successfully 
                  and unsuccessfully loaded tables and any new incoming columns.
        """

        required_params = ["user", "password", "host", "db", "port"]
        validate_kwargs(kwargs, required_params=required_params, func_name="load_warehouse")

        redshift_params = {
            "user": kwargs["user"],
            "password": kwargs["password"],
            "host": kwargs["host"],
            "port": kwargs["port"],
            "database": kwargs["db"],
        }
        engine = Destination.redshift(redshift_params)
        failed_tables = []
        successful_tables = []
        new_incoming = {}
        counter = 0

        #  = create_engine(f'postgresql://root:root@172.18.0.2:5432/postgres') #initialize enine
        for collection in self.data.keys():
            if collection != "metadata":
                ok, message, new_incoming_columns = self.insert_update_record(
                    engine=engine,
                    df=self.data[collection],
                    targetTable=collection,
                    pk="_id",
                )
                # Loader.insert_update_record
                if not ok:
                    print(f"table '{collection}' failed - {message}")
                    failed_tables.append(collection)
                else:
                    successful_tables.append(collection)
                    new_incoming[collection] = new_incoming_columns
                    print(f"table '{collection}' succesful - {message}")

                counter += 1

        outcome = {
            "successful_tables": successful_tables,
            "failed_tables": failed_tables,
            "message": f"{len(successful_tables)}/{counter} tables done.",
            "description": "Redshift load job result",
            "New_incoming_column": new_incoming
        }

        update_loader_status(mongo_conn=self.mongo_conn)
        engine.dispose()
        return outcome

    def run(self, **kwargs):

        """
            Executes the data loading process.

            This method can load data into a data lake, a data warehouse, or both,
            depending on the provided arguments.

            Args:
                datalake (bool): If True, loads data into a data lake.
                warehouse (bool): If True, loads data into a data warehouse.
                kwargs: Additional keyword arguments.

            Returns:
                dict: A summary of the data loading operations.
        """
        # docs should clear on what kwargs want to achieve

        run_details = {}

        # if datalake:
        #     run_details["datalake"] = self.load_datalake(**kwargs)

        if self.datalake and self.warehouse:
            #datalake load
            if not self.aws:
                raise OplogWorksError('Loader.run', 'aws credentials were not provide for datalake operations.')
            
            if type(self.aws) != dict:
                raise OplogWorksError('Loader.run aws cloud validation', 'cloud params provided must be of type dict.')
            
            #ensure we have the required things for aws connect
            required_params = ["aws_access_key_id", "aws_secret_access_key"]
            missing_aws_params = [param for param in required_params if param not in self.aws.keys()]
            if missing_aws_params:
                raise OplogWorksError('Loader.run aws cloud validation', f'All of |{"| ".join(required_params)}| needed for aws connection.')
            
            run_details["datalake"] = self.load_datalake(**kwargs)

            #dw load
            run_details["datawarehouse"] = self.load_warehouse(**kwargs)  
                      
        elif self.warehouse and not self.datalake:
            run_details["datawarehouse"] = self.load_warehouse(**kwargs)
            # write metadata
        return run_details


