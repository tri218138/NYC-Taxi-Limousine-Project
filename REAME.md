Run in order:

```bash
ls
#---
code dataset metastore_db derby.log download-dataset.sh push-dataset-to-hadoop.sh table-schema-creation.txt
```

```bash
chmod +x download-dataset.sh
./download-dataset.sh
```

```bash
chmod +x push-dataset-to-hadoop.sh
./push-dataset-to-hadoop.sh
```

```hive
hive
#--- You are in hive environment
$hive> CREATE DATABASE IF NOT EXISTS NYC_Taxi_Limousine;
$hive> USE NYC_Taxi_Limousine;
$hive> set hivevar:table_name=yellow_tripdata;
$hive> 
CREATE EXTERNAL TABLE ${hivevar:table_name} (
    VendorID INT,
    tpep_pickup_datetime STRING,
    tpep_dropoff_datetime STRING,
    passenger_count DOUBLE,
    trip_distance DOUBLE,
    RatecodeID DOUBLE,
    store_and_fwd_flag STRING,
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE,
    Airport_fee DOUBLE
)
STORED AS PARQUET
LOCATION '~/Downloads/NYC-Taxi-Limousine-Project/table/${hivevar:table_name}.parquet';
```

Now, load your simple data file to data table
```hive
LOAD DATA INPATH '/NYC-Taxi-Limousine-Project/yellow_tripdata_2022-01.parquet' INTO TABLE ${hivevar:table_name};
```
Insert another data file
```hive
INSERT INTO TABLE ${hivevar:table_name}
SELECT * FROM parquet'/NYC-Taxi-Limousine-Project/yellow_tripdata_2022-02.parquet';
```
Or using loop

Hadoop & Hive launch
```bash
./stop-all.sh
$ hadoop namenode -format
./start-all.sh
```

confirm jps contains DataNode
```bash
jps
```

```bash
$ hadoop fs -mkdir hdfs://localhost:9000/NYC-Taxi-Limousine-Project
$ hadoop fs -put Downloads/NYC-Taxi-Limousine-Project/dataset/yellow_tripdata_2022-01.parquet hdfs://localhost:9000/NYC-Taxi-Limousine-Project/yellow_tripdata_2022-01.parquet
```

```bash
apache-hive-3.1.3-bin/bin$ hiveserver2
```

```bash
apache-hive-3.1.3-bin/bin$ beeline
apache-hive-3.1.3-bin/bin$ !connect jdbc:hive2://localhost:10000
```

run source code
```bash
Downloads/NYC-Taxi-Limousine-Project/code$ source venv/bin/activate
```

