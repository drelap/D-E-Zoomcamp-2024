
## Homework

**Please setup the environment in [Getting Started](https://github.com/risingwavelabs/risingwave-data-talks-workshop-2024-03-04?tab=readme-ov-file#getting-started) and for the [Homework](https://github.com/risingwavelabs/risingwave-data-talks-workshop-2024-03-04/blob/main/homework.md#setting-up) first.**


## Question 0

_This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution._

What are the dropoff taxi zones at the latest dropoff times?

For this part, we will use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/).

<details>
<summary>Solution</summary>

```sql
CREATE MATERIALIZED VIEW latest_dropoff_time AS
    WITH t AS (
        SELECT MAX(tpep_dropoff_datetime) AS latest_dropoff_time
        FROM trip_data
    )
    SELECT taxi_zone.Zone as taxi_zone, latest_dropoff_time
    FROM t,
            trip_data
    JOIN taxi_zone
        ON trip_data.DOLocationID = taxi_zone.location_id
    WHERE trip_data.tpep_dropoff_datetime = t.latest_dropoff_time;

--    taxi_zone    | latest_dropoff_time
-- ----------------+---------------------
--  Midtown Center | 2022-01-03 17:24:54
-- (1 row)
```

</details>

### Question 1

Create a materialized view to compute the average, min and max trip time between each taxi zone.

From this MV, find the pair of taxi zones with the highest average trip time.
You may need to use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) for this.

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute,
but the max trip time is 10 minutes and 20 minutes respectively.

```
dev=> SELECT
dev->   pickup_zone,
dev->   dropoff_zone,
dev->   avg_trip_time
dev-> FROM
dev->   taxi_trips_stats
dev-> ORDER BY
dev->   avg_trip_time DESC
dev-> LIMIT 1;
  pickup_zone   | dropoff_zone | avg_trip_time 
----------------+--------------+---------------
 Yorkville East | Steinway     | 23:59:33
(1 row)
```

Options:
1. Yorkville East, Steinway <<==
2. Murray Hill, Midwood
3. East Flatbush/Farragut, East Harlem North
4. Midtown Center, University Heights/Morris Heights

### Question 2

Recreate the MV(s) in question 1, to also find the number of trips for the pair of taxi zones with the highest average trip time.

```
dev=> WITH max_profit AS (SELECT max(avg_trip_time) max FROM taxi_trips_stats)
dev-> SELECT * from taxi_trips_stats, max_profit
dev-> WHERE avg_trip_time >= max;
 pickup_taxi_zone | dropoff_taxi_zone | min_trip_time | max_trip_time | avg_trip_time | cnt |   max
------------------+-------------------+---------------+---------------+---------------+-----+----------
 Yorkville East   | Steinway          | 23:59:33      | 23:59:33      | 23:59:33      |   1 | 23:59:33
(1 row)
```

Options:
1. 5
2. 3
3. 10
4. 1 <<==

### Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 12:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 11:00:00 to 2020-01-01 12:00:00.

HINT: You can use [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/)
to create a filter condition based on the latest pickup time.

NOTE: For this question `17 hours` was picked to ensure we have enough data to work with.

```
dev=> SELECT
dev->   taxi_zone.Zone AS pickup_zone,
dev->   COUNT(*) AS num_rides
dev-> FROM
dev->   trip_data
dev->   JOIN taxi_zone ON taxi_zone.location_id = trip_data.pulocationid
dev-> WHERE
dev->   trip_data.tpep_pickup_datetime >= (
dev(>     SELECT
dev(>       pickup_time - INTERVAL '17 HOURS'
dev(>     FROM
dev(>       latest_pickup_time
dev(>   )
dev-> GROUP BY
dev->   pickup_zone
dev-> ORDER BY
dev->   num_rides DESC
dev-> LIMIT
dev->   3;

     pickup_zone     | num_rides
---------------------+-----------
 LaGuardia Airport   |        19
 Lincoln Square East |        17
 JFK Airport         |        17
(3 rows)
```

Options:
1. Clinton East, Upper East Side North, Penn Station
2. LaGuardia Airport, Lincoln Square East, JFK Airport <<==
3. Midtown Center, Upper East Side South, Upper East Side North
4. LaGuardia Airport, Midtown Center, Upper East Side North


## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/workshop2
- Deadline: 11 March (Monday), 23:00 CET 

## Rewards 🥳

Everyone who completes the homework will get a pen and a sticker, and 5 lucky winners will receive a Tshirt and other secret surprises!
We encourage you to share your achievements with this workshop on your socials and look forward to your submissions 😁

- Follow us on **LinkedIn**: https://www.linkedin.com/company/risingwave
- Follow us on **GitHub**: https://github.com/risingwavelabs/risingwave
- Join us on **Slack**: https://risingwave-labs.com/slack

See you around!


## Solution
