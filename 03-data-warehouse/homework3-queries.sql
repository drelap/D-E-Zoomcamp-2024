  -- Q1:
SELECT COUNT(1) FROM `polished-engine-412507.ny_taxi.external_green_tripdata_2022`


  ------------------------------------------------------------  

  -- Q2:

SELECT DISTINCT PULocationID FROM `polished-engine-412507.ny_taxi.external_green_tripdata_2022`

SELECT DISTINCT PULocationID FROM `polished-engine-412507.ny_taxi.green_tripdata_2022_non_partitioned`

0 MB for the External Table and 6.41MB for the Materialized Table


  ------------------------------------------------------------  

  -- Q3:

SELECT
  COUNT(VendorID)
FROM
  `polished-engine-412507.ny_taxi.external_green_tripdata_2022`
WHERE
  fare_amount = 0


  ------------------------------------------------------------  

  -- Q4:

CREATE OR REPLACE TABLE
  `polished-engine-412507.ny_taxi.green_tripdata_2022_partitioned`
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT
  *
FROM
  `polished-engine-412507.ny_taxi.external_green_tripdata_2022`;

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


  ------------------------------------------------------------  

  -- Q5:

SELECT
  DISTINCT(PULocationID)
FROM
  `polished-engine-412507.ny_taxi.green_tripdata_2022_non_partitioned`
WHERE
  DATE(lpep_pickup_datetime) BETWEEN '2022-06-01'
  AND '2022-06-30';

SELECT
  DISTINCT(PULocationID)
FROM
  `polished-engine-412507.ny_taxi.green_tripdata_2022_partitioned`
WHERE
  DATE(lpep_pickup_datetime) BETWEEN '2022-06-01'
  AND '2022-06-30';








