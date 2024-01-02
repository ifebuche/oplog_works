import boto3
import certifi
import psycopg2
import pymongo
# import snowflake.connector
from sqlalchemy import create_engine
from MI_ETL.Error import OplogWorksError


class Source:
    @staticmethod
    def mongo(mongo_url):
        """connects to mongo using uri

        Args:
            mongo_url (str): mongo connection string

        Returns:
            client: connection object
        """
        print("Connecting to MongoDB...")
        try:
            client = pymongo.MongoClient(mongo_url, tlsCAFile=certifi.where())
            # force a connection to test if the uri is valid
            client.server_info()
            print("Connected")
        except pymongo.errors.ServerSelectionTimeoutError as e:
            raise ConnectionError(f"Failed to connect to MongoDB: {e}")

        return client

    def dynamo(dynamo_details):
        print(f"Connecting to DynamoDB...")
        dynamodb_client = boto3.client(
            "dynamodb",
            region_name=dynamo_details["region"],
            aws_access_key_id=dynamo_details["access_key"],
            aws_secret_access_key=dynamo_details["secret_key"],
        )
        return dynamodb_client

    def cosmodb(cosmos_url):
        print(f"Connecting to CosmosDB...")
        cosmos_client = None
        return cosmos_client


class Destination:
    def s3(access_key, secret_key):
        print(f"Connecting to S3")
        s3_client = boto3.client(
            "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
        )
        return s3_client

    # def snowflake(snowflake_details):
    #     print(f"Connecting to Snowflake")
    #     conn = snowflake.connector.connect(
    #         user=snowflake_details["user"],
    #         password=snowflake_details["password"],
    #         account=snowflake_details["account"],
    #         warehouse=snowflake_details["warehouse"],
    #         database=snowflake_details["database"],
    #         schema=snowflake_details["schema"],
    #     )
    #     return conn

    @staticmethod
    def redshift(redshift_details):
        print("connecting to redshift")
        try:
            conn = create_engine(
                f'postgresql://{redshift_details["user"]}:{redshift_details["password"]}@'
                f'{redshift_details["host"]}:{redshift_details["port"]}/'
                f'{redshift_details["database"]}'
            )
            return conn
        except Exception as e:
            print("Error connection to data warehouse")
            raise OplogWorksError('Destination.redhsift', str(e))
