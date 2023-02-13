## Main goals:
- Extract data from link and load it to a GCS
- Create an external table in BigQuery using data in GCS
- Answer to the question in the BigQuery

### To extract and load data for every month in 2019 I was using Prefect with this parameters

![parameters.png](https://github.com/zdoryk/zoomcamp-de-course/blob/main/Week4_BigQuery/parameters.png)

### All subflows completed successfully

![rada_view.png](https://github.com/zdoryk/zoomcamp-de-course/blob/main/Week4_BigQuery/radar_view.png)

### And all the files were in the gcs

### Using the command below created a new external table from files in GCS

![create_external_table.png](https://github.com/zdoryk/zoomcamp-de-course/blob/main/Week4_BigQuery/create_external_table.png)

### After that I could start to answer the actual questions of homework

### Question 1
What is the count for fhv vehicle records for year 2019?
- 65,623,481
- 43,244,696
- 22,978,333
- 13,942,414

``` SQL
select count (1)
from `dtc-de-course-376115.dezoomcamp.external_fhv`
```

### Answer: 43,244,696

### Question 2 
Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 25.2 MB for the External Table and 100.87MB for the BQ Table
- 225.82 MB for the External Table and 47.60MB for the BQ Table
- 0 MB for the External Table and 0MB for the BQ Table
- 0 MB for the External Table and 317.94MB for the BQ Table

``` bigquery
select distinct affiliated_base_number
from `dtc-de-course-376115.dezoomcamp.external_fhv`
```

### Answer: 0 MB for the External Table and 317.94MB for the BQ Table

### Question 3
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

- 717,748
- 1,215,687
- 5
- 20,332

``` bigquery
select count(1)
from `dtc-de-course-376115.dezoomcamp.external_fhv`
where PUlocationID is null
and DOlocationID is null
```

### Answer: 717,748

### Question 4
What is the best strategy to optimize the table if query always 
filter by pickup_datetime and order by affiliated_base_number?

- Cluster on pickup_datetime Cluster on affiliated_base_number
- Partition by pickup_datetime Cluster on affiliated_base_number
- Partition by pickup_datetime Partition by affiliated_base_number
- Partition by affiliated_base_number Cluster on pickup_datetime

### Answer: Partition by pickup_datetime Cluster on affiliated_base_number

### Question 5

Implement the optimized solution you chose for question 4. 
Write a query to retrieve the distinct affiliated_base_number between 
pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).
Use the BQ table you created earlier in your from clause and note the 
estimated bytes. Now change the table in the from clause to the partitioned t
able you created for question 4 and note the estimated bytes processed. 
What are these values? Choose the answer which most closely matches.

- 12.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
- 582.63 MB for non-partitioned table and 0 MB for the partitioned table
- 646.25 MB for non-partitioned table and 646.25 MB for the partitioned table

``` bigquery

SELECT DISTINCT affiliated_base_number
from `dtc-de-course-376115.dezoomcamp.fhv_partitioned_tripdata`
where pickup_datetime BETWEEN '2019-01-01' AND '2019-03-31'
```

### Answer: 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

### Question 6

Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Container Registry
- Big Table

### Answer: GCP Bucket

### Question 7
It is best practice in Big Query to always cluster your data:


- True
- False

### Answer: false
