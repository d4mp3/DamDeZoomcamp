import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    data = pd.DataFrame()

    years = ['2022']
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
    url_prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_'
    file_extension = '.parquet'

    for year in years:
        for month in months:
            path = f'{url_prefix}{year}-{month}{file_extension}'
            monthly_table = pd.read_parquet(path)
            data = pd.concat([data, monthly_table])

    return data
  

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
