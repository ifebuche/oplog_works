class Connector:
    class Source:
        @staticmethod
        def mongo(mongo_url):
            print("Connecting to MongoDB...")
        
        @staticmethod
        def dynamo(dynamo_details):
            print(f"Connecting to DynamoDB...")

        @staticmethod
        def cosmodb(cosmos_url):
            print(f"Connecting to CosmosDB...")
            
    class Destination:
        @staticmethod
        def s3(bucket_name):
            print(f"Connecting to S3")

        @staticmethod
        def snowflake(snowflake_details):
            print(f"Connecting to Snowflake")

        @staticmethod
        def redshift(redshift_details):
            print(f"Connecting to Redshift")



