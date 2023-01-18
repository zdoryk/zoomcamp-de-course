-- Question 3. Count records
/* How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15.

Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns
are in the format timestamp (date and hour+min+sec) and not in date.

20689
    20530  V
17630
21090

 */


SELECT count(1), cast(lpep_pickup_datetime as date) as "day"
from
    gree_taxi_data_2019_01
where cast(lpep_pickup_datetime as date) = '2019-01-15'
and cast(lpep_dropoff_datetime as date) = '2019-01-15'
GROUP BY day;



/*
 Question 4. Largest trip for each day
Which was the day with the largest trip distance Use the pick up time for your calculations.

2019-01-18
2019-01-28
    2019-01-15 V
2019-01-10

 */

SELECT
    cast(lpep_pickup_datetime as date) as "day",
    max(trip_distance)
FROM
    gree_taxi_data_2019_01
GROUP BY "day"
ORDER BY max(trip_distance) desc;


/*

Question 5. The number of passengers
In 2019-01-01 how many trips had 2 and 3 passengers?

2: 1282 ; 3: 266
2: 1532 ; 3: 126
    2: 1282 ; 3: 254  V
2: 1282 ; 3: 274

 */

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



/*
 Question 6. Largest tip
For the passengers picked up in the Astoria Zone which was the drop off
 zone that had the largest tip? We want the name of the zone, not the id.

Note: it's not a typo, it's tip , not trip

Central Park
Jamaica
South Ozone Park
    Long Island City/Queens Plaza v

 */

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


