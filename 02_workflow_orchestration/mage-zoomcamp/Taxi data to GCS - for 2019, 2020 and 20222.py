import io
import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq

from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path
import itertools

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    services = ["yellow", "green"]
    years = ["2019", "2020", "2022"]
    months = list(i for i in range(1, 13))
    
    config_path = path.join(get_repo_path(), "io_config.yaml") 
    config_profile = "default"

    bucket_name = "mage-zoomcamp-pd2669"

    for service, year, month in itertools.product(services, years, months):
        print(f"Now processing:\nService: {service}, Year: {year}, Month: {month}")
        month = f"{month:02d}"
        file_name = f"{service}_tripdata_{year}-{month}.parquet"
        request_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
        object_key = f"{service}/{year}/{service}_tripdata_{year}_{month}.parquet"

        print(f"request url: {request_url}")
        
        try:
            response = requests.get(request_url)
            response.raise_for_status()
            data = io.BytesIO(response.content)
            
            df = pq.read_table(data).to_pandas()
            print(f"Parquet loaded:\n{file_name}\nDataFrame shape:\n{df.shape}")
    
        except requests.HTTPError as e:
            print(f"HTPP Error: {e}")    


        GoogleCloudStorage.with_config(
            ConfigFileLoader(
                config_path,
                config_profile
                )
            ).export(
                df,
                bucket_name,
                object_key
            )


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

Example of external table creation statement for a single year:

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
