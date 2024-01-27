Q1:

```
andre@localhost:~$ docker run --help|grep "after exit"
      --rm                                       Remove container and any anonymous unnamed volume associated with the container after exit
```


Answer
  --rm

---



Q2:

```
root@886048121a00:/# pip list
Package    Versionpip        23.0.1
setuptools 58.1.0
wheel      0.42.0[notice] A new release of pip is available: 23.0.1 -> 23.3.2
[notice] To update, run: pip install --upgrade pip
root@886048121a00:/#
```


Answer:
0.42.0

---



Q3:

```
SELECT
  COUNT(*)
FROM
  green_taxi_data
WHERE
  lpep_pickup_datetime::date = '2019-09-18' and
  lpep_dropoff_datetime::date = '2019-09-18'
```

Answer:
15612

---



Q4:

```
SELECT
  lpep_pickup_datetime::date as pickup_date,
  sum(trip_distance) as total_trip_distance
FROM
  green_taxi_data
GROUP BY pickup_date
ORDER BY total_trip_distance DESC
```

Answer:
2019-09-26

---



Q5:

```
SELECT "Borough", SUM(total_amount)
FROM
  green_taxi_data JOIN zones
  ON "PULocationID" = "LocationID"
WHERE
  lpep_pickup_datetime::date = '2019-09-18'
GROUP BY "Borough"
HAVING SUM(total_amount) >= 50000;
```

Answer:
Brooklyn 96330.18, Manhattan 92269.36, Queens 78673.07

---



Q6:

```
SELECT "Zone", tip_amount
FROM green_taxi_data JOIN zones
  ON "DOLocationID" = "LocationID"
WHERE
  lpep_pickup_datetime::date BETWEEN '2019-09-01' AND '2019-09-30' AND
  "PULocationID" = (
    SELECT "LocationID" FROM zones WHERE "Zone" = 'Astoria'
  )
ORDER BY tip_amount DESC
```

Answer:
JFK Airport 62.31

---



Q7:

```
andre@localhost:~/projects/D-E-Zoomcamp/terrademo$ terraform apply
google_bigquery_dataset.demo_dataset: Refreshing state... [id=projects/polished-engine-412507/datasets/terraform_demo_dataset]

Terraform used the selected providers to generate the following execution plan. Resource actions are
indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_storage_bucket.demo-bucket will be created
  + resource "google_storage_bucket" "demo-bucket" {
      + effective_labels            = (known after apply)
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "ASIA"
      + name                        = "polished-engine-412507-terra-bucket"
      + project                     = (known after apply)
      + public_access_prevention    = (known after apply)
      + rpo                         = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + terraform_labels            = (known after apply)
      + uniform_bucket_level_access = (known after apply)
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type = "AbortIncompleteMultipartUpload"
            }
          + condition {
              + age                   = 1
              + matches_prefix        = []
              + matches_storage_class = []
              + matches_suffix        = []
              + with_state            = (known after apply)
            }
        }
    }

Plan: 1 to add, 0 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_storage_bucket.demo-bucket: Creating...
google_storage_bucket.demo-bucket: Creation complete after 3s [id=polished-engine-412507-terra-bucket]

Apply complete! Resources: 1 added, 0 changed, 0 destroyed.
andre@localhost:~/projects/D-E-Zoomcamp/terrademo$ 
```

