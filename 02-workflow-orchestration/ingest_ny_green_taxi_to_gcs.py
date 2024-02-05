import pyarrow as pa
import pyarrow.parquet as pq
from pandas import DataFrame
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

# update the variables below
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/magic-zoomcamp/gcp-key.json'
project_id = 'polished-engine-412507'
bucket_name = 'polished-engine-412507-bucket'
object_key = 'ny_green_taxi_data.parquet'
table_name = 'ny_green_taxi_data'
root_path = f'{bucket_name}/{table_name}'


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    
    table = pa.Table.from_pandas(df)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs
    )
