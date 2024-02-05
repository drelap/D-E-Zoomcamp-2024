import io
import pandas as pd
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    year = '2020'
    months = ['10', '11', '12']
    all_df = pd.DataFrame()

    for mon in months: 
        url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{mon}.csv.gz'
        df = pd.read_csv(url, compression='gzip')
        
        all_df = pd.concat([all_df, df], ignore_index=True)
        print(mon, df.shape, "full:", all_df.shape)

    return all_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
