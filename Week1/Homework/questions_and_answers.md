# Week 1 Homework
## In this homework we'll prepare the environment and practice with Docker and SQL

## Question 1. Knowing docker tags
Run the command to get information on Docker 

docker --help

Now run the command to get help on the "docker build" command

Which tag has the following text? - Write the image ID to the file

* --imageid string
* --iidfile string
* --idimage string
* --idfile string

## Answer:
### --iidfile string
Listing example:
``` bash
   -f, --file string                   Name of the Dockerfile (default:
                                      "PATH/Dockerfile")
      --iidfile string                Write the image ID to the file

```

## Question 2. Understanding docker first run
Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use pip list). How many python packages/modules are installed?

* 1
* 6
* 3
* 7

## Answer:
### 3
Listing example:
``` bash
user@user:~/Projects/Zoomcamp/zoomcamp-de-course/Week1$ sudo docker run -it python:3.9 bash
root@337bcb8baec1:/# pip list
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4

```

## Postgres part:
### Prepare Postgres
Run Postgres and load data as shown in the videos We'll use the green taxi trips from January 2019:

wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz

You will also need the dataset with zones:

wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

## Question 3. Count records
How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15.

Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.

* 20689
* 20530
* 17630
* 21090

## Answer:
### 20530
Code:
``` SQL
  SELECT count(1), cast(lpep_pickup_datetime as date) as "day"
  from
      gree_taxi_data_2019_01
  where cast(lpep_pickup_datetime as date) = '2019-01-15'
  and cast(lpep_dropoff_datetime as date) = '2019-01-15'
  GROUP BY day;
```

## Question 4. Largest trip for each day
Which was the day with the largest trip distance Use the pick up time for your calculations.

* 2019-01-18
* 2019-01-28
* 2019-01-15
* 2019-01-10

## Answer:
### 2019-01-15
``` SQL
  SELECT
    cast(lpep_pickup_datetime as date) as "day",
    max(trip_distance)
  FROM
      gree_taxi_data_2019_01
  GROUP BY "day"
  ORDER BY max(trip_distance) desc;

```

## Question 5. The number of passengers
In 2019-01-01 how many trips had 2 and 3 passengers?

* 2: 1282 ; 3: 266
* 2: 1532 ; 3: 126
* 2: 1282 ; 3: 254
* 2: 1282 ; 3: 274

## Answer:
### 2: 1282 ; 3: 254
``` SQL

SELECT
    passenger_count as "passengers_count",
    count(1)
FROM
    gree_taxi_data_2019_01
where passenger_count = 2
  and cast(lpep_pickup_datetime as date) = '2019-01-01'
group by passenger_count

UNION ALL

SELECT
    passenger_count as "passengers_counte",
    count(1)
FROM
    gree_taxi_data_2019_01
where passenger_count = 3
  and cast(lpep_pickup_datetime as date) = '2019-01-01'
group by passenger_count;

```

## Question 6. Largest tip
For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

Note: it's not a typo, it's tip , not trip

* Central Park
* Jamaica
* South Ozone Park
* Long Island City/Queens Plaza

## Answer:
### Long Island City/Queens Plaza
``` SQL
  
  SELECT
      do_zone."Zone",
      max(tip_amount)
  FROM
      gree_taxi_data_2019_01
  LEFT JOIN zones pu_zone on gree_taxi_data_2019_01."PULocationID" = pu_zone."LocationID"
  LEFT JOIN zones do_zone on gree_taxi_data_2019_01."DOLocationID" = do_zone."LocationID"
  WHERE pu_zone."Zone" = 'Astoria'
  GROUP BY do_zone."Zone"
  ORDER BY max(tip_amount) desc
  LIMIT 1
```


