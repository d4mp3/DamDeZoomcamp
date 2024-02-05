if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import re

def camel_to_snake(column_name):
    import re
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', column_name).lower()

@transformer
def transform(data, *args, **kwargs):
    vendor_id_vals = data['VendorID'].unique()
    columns_to_rename = [col for col in data.columns if col != camel_to_snake(col)]
    print(f'VendorID values: {vendor_id_vals}')
    print(f'Columns with CamelCase: {columns_to_rename}')
    

    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    print(len(data['lpep_pickup_date'].unique()))
   

    data.columns = data.columns.map(camel_to_snake)
    return data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]


@test
def test_output(output, *args) -> None:
    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with zero passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with trip distance equal zero'
    assert 'vendor_id' in output.columns, "DataFrame doesn't contain 'vendor_id' column"