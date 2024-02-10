  -- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE
  `polished-engine-412507.ny_taxi.external_green_tripdata_2022` OPTIONS ( format = 'parquet',
    uris = ['gs://polished-engine-412507-bucket/ny_green_tripdata_2022/d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet',
    'gs://polished-engine-412507-bucket/ny_green_tripdata_2022/d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet',
    'gs://polished-engine-412507-bucket/ny_green_tripdata_2022/d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet',
    'gs://polished-engine-412507-bucket/ny_green_tripdata_2022/d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet',
    'gs://polished-engine-412507-bucket/ny_green_tripdata_2022/d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-05.parquet',
    'gs://polished-engine-412507-bucket/ny_green_tripdata_2022/d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-06.parquet',
    'gs://polished-engine-412507-bucket/ny_green_tripdata_2022/d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-07.parquet',
    'gs://polished-engine-412507-bucket/ny_green_tripdata_2022/d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-08.parquet',
    'gs://polished-engine-412507-bucket/ny_green_tripdata_2022/d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-09.parquet',
    'gs://polished-engine-412507-bucket/ny_green_tripdata_2022/d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-10.parquet',
    'gs://polished-engine-412507-bucket/ny_green_tripdata_2022/d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-11.parquet',
    'gs://polished-engine-412507-bucket/ny_green_tripdata_2022/d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-12.parquet'] );


  -- Creating BigQuery non partitioned table
CREATE OR REPLACE TABLE
  `polished-engine-412507.ny_taxi.green_tripdata_2022_non_partitioned` AS
SELECT
  *
FROM
  `polished-engine-412507.ny_taxi.external_green_tripdata_2022`;


  -- Creating BigQuery partitioned table
CREATE OR REPLACE TABLE
  `polished-engine-412507.ny_taxi.green_tripdata_2022_partitioned`
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT
  *
FROM
  `polished-engine-412507.ny_taxi.external_green_tripdata_2022`;


  -- Creating BigQuery partitioned and clustered table
CREATE OR REPLACE TABLE
  `polished-engine-412507.ny_taxi.green_tripdata_2022_partitioned_clustered`
PARTITION BY
  DATE(lpep_pickup_datetime)
CLUSTER BY
  PULocationID AS
SELECT
  *
FROM
  `polished-engine-412507.ny_taxi.external_green_tripdata_2022`;


