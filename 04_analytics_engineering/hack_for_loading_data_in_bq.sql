-- 2019

CREATE TABLE`de-zoomcamp-412512.trips_data_all.green_tripdata` as 
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2019`;

INSERT INTO `de-zoomcamp-412512.trips_data_all.green_tripdata` 
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2020`;

-- 2020

CREATE TABLE`de-zoomcamp-412512.trips_data_all.yellow_tripdata` as 
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2020`;

INSERT INTO `de-zoomcamp-412512.trips_data_all.yellow_tripdata` 
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2020`;

