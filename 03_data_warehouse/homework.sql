-- SETUP

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-412512.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'Parquet',
  uris = ['gs://de-zoomcamp-d4m/nyc_green_taxi_data.parquet']
);

LOAD DATA INTO `de-zoomcamp-412512.ny_taxi.green_tripdata`
FROM FILES (
  format = 'Parquet',
  uris = ['gs://de-zoomcamp-d4m/nyc_green_taxi_data.parquet']
);

-- 1

SELECT COUNT(*) FROM `de-zoomcamp-412512.ny_taxi.external_green_tripdata`;

-- 2

SELECT DISTINCT PULocationID FROM `de-zoomcamp-412512.ny_taxi.external_green_tripdata`;

SELECT DISTINCT PULocationID FROM `de-zoomcamp-412512.ny_taxi.green_tripdata`;

-- 3

SELECT COUNT(*) FROM `de-zoomcamp-412512.ny_taxi.green_tripdata`
where fare_amount = 0;

-- 4

CREATE OR REPLACE TABLE ny_taxi.green_tripdata_partitoned_clustered
PARTITION BY DATE(cleaned_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT 
    *, 
    TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS cleaned_pickup_datetime, 
    TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS cleaned_dropoff_datetime 
FROM ny_taxi.external_green_tripdata;


-- 5

CREATE OR REPLACE TABLE `ny_taxi.green_tripdata_timestamp`
AS
SELECT 
  *, 
  TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64)) AS cleaned_pickup_datetime,
  TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64)) AS cleaned_dropoff_datetime, 
FROM (
  SELECT *
  FROM `de-zoomcamp-412512.ny_taxi.green_tripdata`
);


SELECT DISTINCT PULocationID FROM `de-zoomcamp-412512.ny_taxi.green_tripdata_timestamp`
WHERE DATE(cleaned_pickup_datetime) BETWEEN '2022-01-06' AND '2022-06-30';


-- Example of external table creation statement for a single year:

DROP TABLE IF EXISTS `big-bliss-411815.ny_taxi.yellow_tripdata_2020`;
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `big-bliss-411815.ny_taxi.yellow_tripdata_2020`
(
  VendorID INTEGER,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count FLOAT,
  trip_distance FLOAT,
  RatecodeID FLOAT,
  store_and_fwd_flag STRING,
  PULocationID INTEGER,
  DOLocationID INTEGER,
  payment_type INTEGER,
  fare_amount FLOAT,
  extra FLOAT,
  mta_tax FLOAT,
  tip_amount FLOAT,
  tolls_amount FLOAT,
  improvement_surcharge FLOAT,
  total_amount FLOAT,
  congestion_surcharge FLOAT,
  airport_fee INTEGER
)
OPTIONS (
  format = 'parquet',
  uris = [
    'gs://mage-zoomcamp-pd2669/yellow/2020/yellow_tripdata_2020_01.parquet',
    'gs://mage-zoomcamp-pd2669/yellow/2020/yellow_tripdata_2020_02.parquet',
    'gs://mage-zoomcamp-pd2669/yellow/2020/yellow_tripdata_2020_03.parquet',
    'gs://mage-zoomcamp-pd2669/yellow/2020/yellow_tripdata_2020_04.parquet',
    'gs://mage-zoomcamp-pd2669/yellow/2020/yellow_tripdata_2020_05.parquet',
    'gs://mage-zoomcamp-pd2669/yellow/2020/yellow_tripdata_2020_06.parquet',
    'gs://mage-zoomcamp-pd2669/yellow/2020/yellow_tripdata_2020_07.parquet',
    'gs://mage-zoomcamp-pd2669/yellow/2020/yellow_tripdata_2020_08.parquet',
    'gs://mage-zoomcamp-pd2669/yellow/2020/yellow_tripdata_2020_09.parquet',
    'gs://mage-zoomcamp-pd2669/yellow/2020/yellow_tripdata_2020_10.parquet',
    'gs://mage-zoomcamp-pd2669/yellow/2020/yellow_tripdata_2020_11.parquet',
    'gs://mage-zoomcamp-pd2669/yellow/2020/yellow_tripdata_2020_12.parquet'
    ]
);

