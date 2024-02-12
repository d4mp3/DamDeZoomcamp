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