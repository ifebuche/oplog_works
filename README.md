# MI-ETLx

**A python module for incremental Extract and Load**

_Data on point - The Smart Way to Extract and Load_

## OVERVIEW

MI-ETL addresses the significant inefficiencies inherent in data pipelines that rely on full loads. Traditional full-load pipelines can lead to bloated payloads, resulting in escalating costs in terms of time, storage, compute resources, and overall expense. By implementing a more efficient and controlled approach to data management, MI-ETL revolutionizes how organizations sustain their data pipelines.

## FEATURES

- Change Data Capture Utilization: MI-ETL leverages MongoDB's change data capture (CDC) feature, the oplog, to move only the data that has changed since the last run. This method ensures minimal data transfer, reducing the payload significantly.

- Controlled Payloads: With MI-ETL, the payload is fully controlled, guaranteeing that time, compute costs, and other metrics are predictable and manageable. This makes budgeting for data pipeline operations more straightforward and efficient.

- Reduced Data Load Sizes: Moving from handling multiple gigabits of data in each full load, MI-ETL potentially reduces the load to less than 50MB per run, a drastic decrease that enhances efficiency.

- Extendable to Various Databases: Initially focused on MongoDB, MI-ETL has the capability to be extended to other types of databases. For example, incorporating DynamoDB streams for capturing DynamoDB change events.

- Empowering Data Engineering Teams: MI-ETL puts control back into the hands of data engineering teams, allowing them to manage their data pipelines more effectively and with greater precision.

<!-- Data pipelines that do full loads are a ticking time bomb building up to when the payload gets bloated out of proportion. This forces organisations to incrementally spend more (time, storage and compute resources, and so cost) to sustain their data pipelines.

MI-ETL takes advantage of the change data capture (CDC) feature in MongoDB, the oplog. This way, it moves only changed objects from source to destination since it's last run. This means that the payload is fully controlled, guaranteeing that time, compute cost and other metrics can easily be determined and budgeted for. Provided DE teams are ready to don their engineering, hats, MI-ETL ensures they take back control of the data pipeline. From moving multiple gigabits of files at each full load, MI-ETL could potentially reduce the load to less than 50MB at each run.

MI-ETL is focused initially on MongoDB but can be extended to included other types of dbs. DynamoDB streams for example could be incorporated for DynamoDB change events. -->

## Supported datastores

<table style="background-color: #fff;">
	<thead>
		<tr>
			<th colspan="2">Integration</th>
			<th>Destination</th>
		</tr>
	</thead>
	<tbody>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/MongoDB-Logo.jpg" />
			</td>
			<td style="width: 200px;">
				<h4>Mongo DB</h4>
			</td>
			<td>
				Data Source
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/datasource_redshift.jpg" />
			</td>
			<td style="width: 200px;">
				<h4>AWS Redshift</h4>
			</td>
			<td>
				Data Sink
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/awss3.jpg" />
			</td>
			<td style="width: 200px;">
				<h4>AWS S3</h4>
			</td>
			<td>
				Data Sink
			</td>
		</tr>
		<tr>
			<td style="text-align: center; height: 40px; background-color: #fff;">
				<img height="40" src="./docs/postgres.jpg" />
			</td>
			<td style="width: 200px;">
				<h4>PostgresSQL</h4>
			</td>
			<td>
				Data Sink
			</td>
		</tr>
	</tbody>
</table>

## INSTALLATION

To see MI-ETLx in action on your own data:

First step is to pip install the MI-ETLx package

```
pip install -i https://test.pypi.org/simple/ MI-ETLx==0.0.36 --extra-index-url https://pypi.org/simple
```

## QUICK START

To use MI-ETLx, a package that extracts incremental data and loads them to target database, lakes and warehouse

### Example 1

```python
from MI_ETL.Connector import Source
from MI_ETL.data_extraction import DataExtraction
from MI_ETL.loader import Loader
import os

host = os.getenv('oplog_test_host')
user = os.getenv('oplog_test_user')
password = os.getenv('oplog_test_password')
db = os.getenv('oplog_test_db')

required_params = {"host":host, "user":user, "password": password, "db":db}

if all(required_params.values()):
    conn = Source.mongo(os.getenv('oplog_test_source_url'))

    data_extraction = DataExtraction(connection=conn, extract_all=[], db='sample_analytics')
    extracted_data = data_extraction.extract_oplog_data()

    #Initiate loader
    # The MongoDB connection is also used to update the time metadata for the next run,
    # ensuring that each run's timing information is accurately recorded.
    loader = Loader(mongo_conn=conn, data=extracted_data, datalake=False, datawarehouse=True, aws={})

    # Provide connection to datawarehouse. NOTE !! only redshift and respective postgres dbs
    # are supported as at this release
    # Result holds meta data information about the load process highlighting if it passed or fail,
    # schema information , e.t.c
    result = loader.run(host=host, user=user, password=password, db=db, port=5432)
else:
    for key, val in required_params.items():
        if not val:
            print(f"'{key}' is needed for the destination database connection")
```

### Example 2

```python
#Example 2
from MI_ETL.Connector import Source
from MI_ETL.data_extraction import DataExtraction
from MI_ETL.loader import Loader
import os

host = os.getenv('oplog_test_host')
user = os.getenv('oplog_test_user')
password = os.getenv('oplog_test_password')
db = os.getenv('oplog_test_db')

required_params = {"host":host, "user":user, "password": password, "db":db}

if all(required_params.values()):
    conn = Source.mongo(os.getenv('oplog_test_source_url'))

    # Intitialize data extraction from specified collections 'collection_1' and 'collection_2' within 'sample_analytics' database.
    data_extraction = DataExtraction(connection=conn, extract_all=['collection_1', 'collection_2'], db='sample_analytics')
    extracted_data = data_extraction.extract_oplog_data()

    #Initiate loader
    # The MongoDB connection is also used to update the time metadata for the next run,
    # ensuring that each run's timing information is accurately recorded.
    loader = Loader(mongo_conn=conn, data=extracted_data, datalake=False, datawarehouse=True, aws={})

    # Provide connection to datawarehouse. NOTE !! only redshift and respective postgres dbs
    # are supported as at this release
    # Result holds meta data information about the load process highlighting if it passed or fail,
    # schema information , e.t.c
    result = loader.run(host=host, user=user, password=password, db=db, port=5432)
else:
    for key, val in required_params.items():
        if not val:
            print(f"'{key}' is needed for the destination database connection")
```

### Example 3

```python
from MI_ETL.Connector import Source
from MI_ETL.data_extraction import DataExtraction
from MI_ETL.loader import Loader
import os

host = os.getenv('oplog_test_host')
user = os.getenv('oplog_test_user')
password = os.getenv('oplog_test_password')
db = os.getenv('oplog_test_db')

required_params = {"host":host, "user":user, "password": password, "db":db}

if all(required_params.values()):
    conn = Source.mongo(os.getenv('oplog_test_source_url'))

    # Initialize data extraction from 'collection_1' and 'collection_2' in 'sample_analytics',
    # extracting from data modified after '2023/12/28' (backfill date).
    data_extraction = DataExtraction(connection=conn, extract_all=['collection_1', 'collection_2'],  db='sample_analytics', backfill='2023/12/28')
    extracted_data = data_extraction.extract_oplog_data()

    #Initiate loader
    # The MongoDB connection is also used to update the time metadata for the next run,
    # ensuring that each run's timing information is accurately recorded.
    loader = Loader(mongo_conn=conn, data=extracted_data, datalake=False, datawarehouse=True, aws={})

    # Provide connection to datawarehouse. NOTE !! only redshift and respective postgres dbs
    # are supported as at this release
    # Result holds meta data information about the load process highlighting if it passed or fail,
    # schema information , e.t.c
    result = loader.run(host=host, user=user, password=password, db=db, port=5432)
else:
    for key, val in required_params.items():
        if not val:
            print(f"'{key}' is needed for the destination database connection")
```
