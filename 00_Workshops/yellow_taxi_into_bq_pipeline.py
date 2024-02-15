import dlt
import pandas as pd

def get_data_from_url():
    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float,
    }

    years = ['2019', '2020']
    # months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    months = ['01', '02', '03']
    parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_'
    file_extension = '.csv.gz'

    for year in years:
        for month in months:
            url = f'{url_prefix}{year}-{month}{file_extension}'
            year_month = f'{year}-{month}'
            data = (year_month, pd.read_csv(url, sep=",", compression="gzip", dtype=taxi_dtypes, parse_dates=parse_dates))
            yield data


def load_taxi_trips(data):
    year_month, csv_data = data
    table_name = f'yellow_taxi_trip_{year_month}'

    pipeline = dlt.pipeline(
        pipeline_name='taxi_pipeline',
        destination='bigquery',
        dataset_name='ny_taxi'
    )

    pipeline.run(
        csv_data,
        table_name=table_name,
        write_disposition="replace"
    )


if __name__ == "__main__":
    # run our main example
    generator = get_data_from_url()
    for data in generator:
        load_taxi_trips(data)
