import boto3
import psycopg2
import pymongo
import certifi
import snowflake.connector


class Connector:

    class Source:

        @staticmethod
        def mongo(mongo_url):
            print("Connecting to MongoDB...")
            client = pymongo.MongoClient(mongo_url, tlsCAFile=certifi.where())
            return client

        @staticmethod
        def dynamo(dynamo_details):
            print(f"Connecting to DynamoDB...")
            dynamodb_client = boto3.client(
                'dynamodb',
                region_name=dynamo_details['region'],
                aws_access_key_id=dynamo_details['access_key'],
                aws_secret_access_key=dynamo_details['secret_key']
            )
            return dynamodb_client

        @staticmethod
        def cosmodb(cosmos_url):
            print(f"Connecting to CosmosDB...")
            cosmos_client = None
            return cosmos_client

    class Destination:

        @staticmethod
        def s3(access_key, secret_key):
            print(f"Connecting to S3")
            s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
            return s3_client

        @staticmethod
        def snowflake(snowflake_details):
            print(f"Connecting to Snowflake")
            conn = snowflake.connector.connect(
                user=snowflake_details['user'],
                password=snowflake_details['password'],
                account=snowflake_details['account'],
                warehouse=snowflake_details['warehouse'],
                database=snowflake_details['database'],
                schema=snowflake_details['schema']
            )
            return conn

        @staticmethod
        def redshift(redshift_details):
            print(f"Connecting to Redshift")
            conn = psycopg2.connect(
                host=redshift_details['host'],
                port=redshift_details['port'],
                database=redshift_details['database'],
                user=redshift_details['user'],
                password=redshift_details['password']
            )
            return conn
        
        