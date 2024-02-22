CREATE TABLE
  `polished-engine-412507.trips_data_all.green_tripdata` AS
SELECT
  *
FROM
  `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2019`;

CREATE TABLE
  `polished-engine-412507.trips_data_all.yellow_tripdata` AS
SELECT
  *
FROM
  `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2019`;

INSERT INTO
  `polished-engine-412507.trips_data_all.green_tripdata`
SELECT
  *
FROM
  `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2020`;

INSERT INTO
  `polished-engine-412507.trips_data_all.yellow_tripdata`
SELECT
  *
FROM
  `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2020`;
