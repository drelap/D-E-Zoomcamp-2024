import pandas as pd
import inflection
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    # change columns to snake_case
    data.columns = data.columns.map(inflection.underscore)

    # filter out rows with passenger_count = 0 or trip_distance = 0
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

    # add a date column based on the date time column
    data.lpep_pickup_datetime= pd.to_datetime(data.lpep_pickup_datetime)
    data.lpep_dropoff_datetime= pd.to_datetime(data.lpep_dropoff_datetime)
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date 
    print(data.shape)
    return data


@test
def test_output(output, *args) -> None:
    assert 'vendor_id' in output.columns, 'vendorid does not exist'
    
@test
def test_output(output, *args) -> None:
    assert output['passenger_count'].isin([0]).sum()==0 , 'There are rides with zero passengers'

@test
def test_output(output, *args) -> None:
    assert output['trip_distance'].isin([0]).sum()==0 , 'There are rides with zero trip distance'