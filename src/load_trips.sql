INSERT INTO NYCTAXI.ODS_TRIP
SELECT DISTINCT
    uuid_generate() AS trip_id
	, tpep_pickup_datetime
	, tpep_dropoff_datetime
	, passenger_count
	, trip_distance
	, PULocationID
	, DOLocationID
	, payment_type
	, fare_amount
	, extra
	, tip_amount
	, tolls_amount
	, total_amount
	, source
	, filename
	, load_ts
FROM NYCTAXI.STG_TRIP
WHERE tpep_dropoff_datetime > tpep_pickup_datetime
	AND trip_distance > 0
	AND PULocationID > 0
	AND DOLocationID > 0
	AND fare_amount > 0
	AND extra > 0
	AND tip_amount > 0
	AND tolls_amount > 0
	AND total_amount > 0;

INSERT INTO NYCTAXI.H_TRIP
SELECT trip_id, source, filename, load_ts
FROM NYCTAXI.V_ODS_TRIP;

INSERT INTO NYCTAXI.S_TRIP_PICKUP
SELECT trip_id, tpep_pickup_datetime, load_ts
FROM NYCTAXI.V_ODS_TRIP;

INSERT INTO NYCTAXI.S_TRIP_DROPOFF
SELECT trip_id, tpep_dropoff_datetime, load_ts
FROM NYCTAXI.V_ODS_TRIP;

INSERT INTO NYCTAXI.S_TRIP_PASS_COUNT
SELECT trip_id, passenger_count, load_ts
FROM NYCTAXI.V_ODS_TRIP
WHERE passenger_count IS NOT NULL;

INSERT INTO NYCTAXI.S_TRIP_DISTANCE
SELECT trip_id, trip_distance, load_ts
FROM NYCTAXI.V_ODS_TRIP
WHERE trip_distance IS NOT NULL;

INSERT INTO NYCTAXI.S_TRIP_FARE
SELECT trip_id, fare_amount, load_ts
FROM NYCTAXI.V_ODS_TRIP
WHERE fare_amount IS NOT NULL;

INSERT INTO NYCTAXI.S_TRIP_EXTRA
SELECT trip_id, extra, load_ts
FROM NYCTAXI.V_ODS_TRIP
WHERE extra IS NOT NULL;

INSERT INTO NYCTAXI.S_TRIP_TIP
SELECT trip_id, tip_amount, load_ts
FROM NYCTAXI.V_ODS_TRIP
WHERE tip_amount IS NOT NULL;

INSERT INTO NYCTAXI.S_TRIP_TOLLS
SELECT trip_id, tolls_amount, load_ts
FROM NYCTAXI.V_ODS_TRIP
WHERE tolls_amount IS NOT NULL;

INSERT INTO NYCTAXI.S_TRIP_TOTAL
SELECT trip_id, total_amount, load_ts
FROM NYCTAXI.V_ODS_TRIP
WHERE total_amount IS NOT NULL;

INSERT INTO NYCTAXI.L_TRIP_PAYMENT_TYPE
SELECT t.trip_id, p.payment_type_id, t.load_ts
FROM NYCTAXI.V_ODS_TRIP t
INNER JOIN NYCTAXI.V_ODS_PAYMENT_TYPE p
ON t.payment_type = p.type_id;

INSERT INTO NYCTAXI.L_TRIP_PU_LOCATION
SELECT t.trip_id, l.loc_id, t.load_ts
FROM NYCTAXI.V_ODS_TRIP t
INNER JOIN NYCTAXI.V_ODS_ZONES l
ON t.PULocationID = l.LocationID;

INSERT INTO NYCTAXI.L_TRIP_DO_LOCATION
SELECT t.trip_id, l.loc_id, t.load_ts
FROM NYCTAXI.V_ODS_TRIP t
INNER JOIN NYCTAXI.V_ODS_ZONES l
ON t.DOLocationID = l.LocationID;

COMMIT;
