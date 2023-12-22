# MI-ETLx
**A python module for incremental ETL on a MongoDB cluster** 

*Data on point - The Smart Way to Extract and Load*

## OVERVIEW

MI-ETL addresses the significant inefficiencies inherent in data pipelines that rely on full loads. Traditional full-load pipelines can lead to bloated payloads, resulting in escalating costs in terms of time, storage, compute resources, and overall expense. By implementing a more efficient and controlled approach to data management, MI-ETL revolutionizes how organizations sustain their data pipelines.

## FEATURES
* Change Data Capture Utilization: MI-ETL leverages MongoDB's change data capture (CDC) feature, the oplog, to move only the data that has changed since the last run. This method ensures minimal data transfer, reducing the payload significantly.

* Controlled Payloads: With MI-ETL, the payload is fully controlled, guaranteeing that time, compute costs, and other metrics are predictable and manageable. This makes budgeting for data pipeline operations more straightforward and efficient.

* Reduced Data Load Sizes: Moving from handling multiple gigabits of data in each full load, MI-ETL potentially reduces the load to less than 50MB per run, a drastic decrease that enhances efficiency.

* Extendable to Various Databases: Initially focused on MongoDB, MI-ETL has the capability to be extended to other types of databases. For example, incorporating DynamoDB streams for capturing DynamoDB change events.

* Empowering Data Engineering Teams: MI-ETL puts control back into the hands of data engineering teams, allowing them to manage their data pipelines more effectively and with greater precision.


<!-- Data pipelines that do full loads are a ticking time bomb building up to when the payload gets bloated out of proportion. This forces organisations to incrementally spend more (time, storage and compute resources, and so cost) to sustain their data pipelines.

MI-ETL takes advantage of the change data capture (CDC) feature in MongoDB, the oplog. This way, it moves only changed objects from source to destination since it's last run. This means that the payload is fully controlled, guaranteeing that time, compute cost and other metrics can easily be determined and budgeted for. Provided DE teams are ready to don their engineering, hats, MI-ETL ensures they take back control of the data pipeline. From moving multiple gigabits of files at each full load, MI-ETL could potentially reduce the load to less than 50MB at each run.

MI-ETL is focused initially on MongoDB but can be extended to included other types of dbs. DynamoDB streams for example could be incorporated for DynamoDB change events. -->

## INSTALLATION
To see MI-ETLx in action on your own data: 

First step is to pip install the MI-ETLx package

```
pip install MI_ETLx
```

## QUICK START
To use MI-ETLx, a package that extracts incremental data and loads them to target database, lakes and warehouse
