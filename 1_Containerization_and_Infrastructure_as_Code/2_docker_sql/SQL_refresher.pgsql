SELECT 
	*
FROM
	yellow_taxi_trips t,
	zones zpu,
	zones zdo
WHERE
	t."PULocationID" = zpu."LocationID" AND
	t."DOLocationID" = zdo."LocationID"
LIMIT 100;

---------------------------------------------------------------------------------------------------

SELECT 
	tpep_pickup_datetime,
    tpep_dropoff_datetime,
    total_amount,
    CONCAT(zpu."Borough", ' / ', zpu."Zone") AS "pick_up_loc",
    CONCAT(zdo."Borough", ' / ', zdo."Zone") AS "drop_off_loc"

FROM
	yellow_taxi_trips t,
	zones zpu,
	zones zdo
WHERE
	t."PULocationID" = zpu."LocationID"
	t."DOLocationID" = zdo."LocationID"
LIMIT 100;
	
---------------------------------------------------------------------------------------------------

SELECT 
	tpep_pickup_datetime,
    tpep_dropoff_datetime,
    total_amount,
    CONCAT(zpu."Borough", ' / ', zpu."Zone") AS "pick_up_loc",
    CONCAT(zdo."Borough", ' / ', zdo."Zone") AS "drop_off_loc"
FROM
	yellow_taxi_trips t 
	JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
	JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
	
LIMIT 100;

---------------------------------------------------------------------------------------------------

SELECT 
	tpep_pickup_datetime,
    tpep_dropoff_datetime,
    total_amount,
   	"PULocationID",
	"DOLocationID"
FROM
	yellow_taxi_trips t 
WHERE
	"PULocationID" is NULL

---------------------------------------------------------------------------------------------------

SELECT 
	tpep_pickup_datetime,
    tpep_dropoff_datetime,
    total_amount,
   	"PULocationID",
	"DOLocationID"
FROM
	yellow_taxi_trips t 
WHERE
	"DOLocationID" NOT IN (SELECT "LocationID" FROM zones);

---------------------------------------------------------------------------------------------------

SELECT 
	tpep_pickup_datetime,
    tpep_dropoff_datetime,
    total_amount,
   	"PULocationID",
	"DOLocationID"
FROM
	yellow_taxi_trips t 
	LEFT JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
	LEFT JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE
	"PULocationID" NOT IN (SELECT "LocationID" FROM zones);

---------------------------------------------------------------------------------------------------

SELECT 
	tpep_pickup_datetime,
    tpep_dropoff_datetime,
	DATE_TRUNC('DAY', tpep_dropoff_datetime),
    total_amount
FROM
	yellow_taxi_trips t;

---------------------------------------------------------------------------------------------------

SELECT 
	tpep_pickup_datetime,
    tpep_dropoff_datetime,
	CAST(tpep_dropoff_datetime AS DATE),
    total_amount
FROM
	yellow_taxi_trips t;

---------------------------------------------------------------------------------------------------

SELECT 
	CAST(tpep_dropoff_datetime AS DATE) as "day",
	COUNT(1) as "amount"
FROM
	yellow_taxi_trips t
GROUP BY
	CAST(tpep_dropoff_datetime AS DATE)
ORDER BY amount DESC;

---------------------------------------------------------------------------------------------------

SELECT 
	CAST(tpep_dropoff_datetime AS DATE) as "day",
	COUNT(1) as "count",
	MAX(total_amount),
	MAX(passenger_count)
FROM
	yellow_taxi_trips t
GROUP BY
	CAST(tpep_dropoff_datetime AS DATE)
ORDER BY "count" DESC;

---------------------------------------------------------------------------------------------------

SELECT 
	CAST(tpep_dropoff_datetime AS DATE) as "day",
	"DOLocationID",
	COUNT(1) as "count",
	MAX(total_amount),
	MAX(passenger_count)
FROM
	yellow_taxi_trips t
GROUP BY
	1, 2
ORDER BY 
	"day" ASC, 
	"DOLocationID" ASC;

---------------------------------------------------------------------------------------------------

SELECT lpep_pickup_datetime, lpep_dropoff_datetime
FROM green_taxi_trips
WHERE DATE(lpep_pickup_datetime)=DATE('2019-09-18') AND DATE(lpep_dropoff_datetime)=DATE('2019-09-18')
ORDER BY lpep_dropoff_datetime ASC;

---------------------------------------------------------------------------------------------------

SELECT 
	lpep_pickup_datetime, 
	trip_distance

FROM green_taxi_trips ORDER BY trip_distance DESC;

---------------------------------------------------------------------------------------------------

SELECT 
	zpu."Borough",
	sum(total_amount) as "amount"
FROM
	green_taxi_trips t 
	JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
WHERE DATE(lpep_pickup_datetime) = DATE('2019-09-18') and zpu."Borough" NOT LIKE 'Unknown'
GROUP BY zpu."Borough"
ORDER BY "amount" DESC;

---------------------------------------------------------------------------------------------------

SELECT 
	zdo."Zone",
	lpep_pickup_datetime,
	tip_amount
FROM
	green_taxi_trips t 
	JOIN zones zpu ON t."PULocationID" = zpu."LocationID"
	JOIN zones zdo ON t."DOLocationID" = zdo."LocationID"
WHERE 
	DATE(lpep_pickup_datetime) >= DATE('2019-09-01') 
AND  
	DATE(lpep_pickup_datetime) <= DATE('2019-09-30')
AND
	zpu."Zone" = 'Astoria'
ORDER BY tip_amount DESC;


---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
