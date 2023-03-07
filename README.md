# MI-ETL
A python module for incremental ETL on a MongoDB cluster

Data pipelines that do full loads are a ticking time bomb building up to when the payload gets bloated out of proportion. This forces organisations to incrementally spend more (time, storage and compute resources, and so cost) to sustain their data pipelines. 

MI-ETL takes advantage change data capture (CDC) feature in MongoDB, the oplog. This way, it moves only changed objects from source to destination since it's last run. This means that the payload is fully controlled, guaranteeing that time, compute cost and other metrics can easily be determined and budgeted for. 

Provided DE teams are ready to don their _engineering_ hats, MI-ETL ensures they take back control of the data pipeline. From moving multiple gigabits of files at each full load, MI-ETL could potentially reduce the load to less than 50MB at each run depending on the business structure.

MI-ETL is focused initially on MongoDB but can be extended to include other types of dbs. DynamoDB streams for example could be incorporated for DynamoDB chnage events.