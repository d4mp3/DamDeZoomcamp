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
    services = ["fhv"]
    years = ["2019"]
    months = list(i for i in range(1, 13))
    
    config_path = path.join(get_repo_path(), "io_config.yaml") 
    config_profile = "default"

    bucket_name = "mage-zoomcamp-d4m"

    

    for service, year, month in itertools.product(services, years, months):
        print(f"Now processing:\nService: {service}, Year: {year}, Month: {month}")
        month = f"{month:02d}"
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"
        request_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{file_name}"
        object_key = f"{service}/{year}/{service}_tripdata_{year}_{month}.csv"

        print(f"request url: {request_url}")
        
        try:
            response = requests.get(request_url)
            response.raise_for_status()
            data = io.BytesIO(response.content)
            
            df = pd.read_csv(data, compression='gzip')
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
