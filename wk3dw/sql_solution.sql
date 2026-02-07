
-- External table means pseudo table that brings the data from cloud storage but store metadata here
create or replace external table `zoomcamp.nyc-external-yl-taxi-data-2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://wk3-dw-warehouse/yellow_tripdata_2024-*.parquet']
);

-- 1
select count(*) from `zoomcamp.nyc-external-yl-taxi-data-2024`

CREATE OR REPLACE TABLE zoomcamp.yellow_tripdata_np_2024 AS
SELECT * FROM `zoomcamp.nyc-external-yl-taxi-data-2024`;

-- 2
-- wrong it only filter non null but not unique value
-- select distinct count(PULocationID) from `zoomcamp.nyc-external-yl-taxi-data-2024`;

-- select distinct count(PULocationID) from `zoomcamp.yellow_tripdata_np_2024`;


-- correct
select distinct count(PULocationID) from `zoomcamp.nyc-external-yl-taxi-data-2024`;

select distinct count(PULocationID) from `zoomcamp.yellow_tripdata_np_2024`;

-- 3
select PULocationID, DOLocationID from `zoomcamp.yellow_tripdata_np_2024`;

-- 4
select count(*) from `zoomcamp.yellow_tripdata_np_2024` where fare_amount = 0;

-- 5
create or replace table zoomcamp.yellow_tripdata_p_2024
PARTITION BY
  DATE(tpep_dropoff_datetime) 
CLUSTER BY VendorID AS 
select * from `zoomcamp.yellow_tripdata_np_2024`;

-- 6
select distinct VendorID from `zoomcamp.yellow_tripdata_np_2024` where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';
-- 310.24 mb
select distinct VendorID from `zoomcamp.yellow_tripdata_p_2024` where date(tpep_dropoff_datetime) between '2024-03-01' and '2024-03-15';
-- 26.84 mb

-- 7
false

-- 8
select count(*) from `zoomcamp.yellow_tripdata_np_2024`;

-- 9
0 because the BQ will bring the answer from the table's metadata instead, no rows scanning required(except querying external table instead, that will be another case)