CREATE SCHEMA IF NOT EXISTS NYCTAXI;

-- Stage
--- Trip records
DROP TABLE IF EXISTS NYCTAXI.STG_TRIP;
CREATE TABLE NYCTAXI.STG_TRIP (
	VendorID					INTEGER
	, tpep_pickup_datetime		DATETIME NOT NULL
	, tpep_dropoff_datetime		DATETIME NOT NULL
	, passenger_count			INTEGER
	, trip_distance				FLOAT
	, RatecodeID				INTEGER
	, store_and_fwd_flag		CHAR
	, PULocationID				INTEGER NOT NULL
	, DOLocationID				INTEGER NOT NULL
	, payment_type				INTEGER
	, fare_amount				FLOAT
	, extra						FLOAT
	, mta_tax					FLOAT
	, tip_amount				FLOAT
	, tolls_amount				FLOAT
	, improvement_surcharge		FLOAT
	, total_amount				FLOAT
	, congestion_surcharge		FLOAT
	, source					VARCHAR(128) NOT NULL
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY (tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID)
) 
ORDER BY tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, load_ts
SEGMENTED BY HASH(tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID) ALL NODES
;

--- Payment type
DROP TABLE IF EXISTS NYCTAXI.STG_PAYMENT_TYPE;
CREATE TABLE NYCTAXI.STG_PAYMENT_TYPE (
	type_id						INTEGER NOT NULL
	, type						VARCHAR(32)
	, source					VARCHAR(128) NOT NULL
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY (type_id)
) 
ORDER BY type_id, load_ts
SEGMENTED BY HASH(type_id) ALL NODES
;

--- Locations (Zones)
DROP TABLE IF EXISTS NYCTAXI.STG_ZONES;
CREATE TABLE NYCTAXI.STG_ZONES (
	LocationID					INTEGER NOT NULL
	, Borough					VARCHAR(64)
	, Zone						VARCHAR(128)
	, service_zone				VARCHAR(64)
	, source					VARCHAR(128) NOT NULL
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY (LocationID)
) 
ORDER BY LocationID, load_ts
SEGMENTED BY HASH(LocationID) ALL NODES
;

-- ODS
--- Trip records
DROP TABLE IF EXISTS NYCTAXI.ODS_TRIP;
CREATE TABLE NYCTAXI.ODS_TRIP (
	trip_id						UUID NOT NULL
	, tpep_pickup_datetime		DATETIME NOT NULL
	, tpep_dropoff_datetime		DATETIME NOT NULL
	, passenger_count			INTEGER
	, trip_distance				FLOAT
	, PULocationID				INTEGER NOT NULL
	, DOLocationID				INTEGER NOT NULL
	, payment_type				INTEGER
	, fare_amount				FLOAT
	, extra						FLOAT
	, tip_amount				FLOAT
	, tolls_amount				FLOAT
	, total_amount				FLOAT
	, source					VARCHAR(128) NOT NULL
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY (trip_id, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID)
) 
ORDER BY trip_id, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, load_ts
SEGMENTED BY HASH(tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_ODS_TRIP AS
SELECT
	trip_id
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
FROM NYCTAXI.ODS_TRIP
LIMIT 1 OVER (PARTITION BY tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID ORDER BY load_ts DESC);

--- Payment type
DROP TABLE IF EXISTS NYCTAXI.ODS_PAYMENT_TYPE;
CREATE TABLE NYCTAXI.ODS_PAYMENT_TYPE (
	payment_type_id				UUID NOT NULL
	, type_id					INTEGER NOT NULL
	, type						VARCHAR(32)
	, source					VARCHAR(128) NOT NULL
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY (payment_type_id, type_id)
) 
ORDER BY payment_type_id, type_id, load_ts
SEGMENTED BY HASH(type_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_ODS_PAYMENT_TYPE AS
SELECT
	payment_type_id
   , type_id
   , type
   , source
   , filename
   , load_ts
FROM NYCTAXI.ODS_PAYMENT_TYPE
LIMIT 1 OVER (PARTITION BY type_id ORDER BY load_ts DESC);

--- Locations (Zones)
DROP TABLE IF EXISTS NYCTAXI.ODS_ZONES;
CREATE TABLE NYCTAXI.ODS_ZONES (
	loc_id						UUID NOT NULL
	, LocationID				INTEGER NOT NULL
	, Borough					VARCHAR(64)
	, Zone						VARCHAR(128)
	, service_zone				VARCHAR(64)
	, source					VARCHAR(128) NOT NULL
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY (loc_id, LocationID)
) 
ORDER BY loc_id, LocationID, load_ts
SEGMENTED BY HASH(LocationID) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_ODS_ZONES AS
SELECT
	loc_id
	, LocationID
	, Borough
	, Zone
	, service_zone
	, source
	, filename
	, load_ts
FROM NYCTAXI.ODS_ZONES
LIMIT 1 OVER (PARTITION BY LocationID ORDER BY load_ts DESC);


-- DDS
-- Anchor for Trip record
DROP TABLE IF EXISTS NYCTAXI.H_TRIP;
CREATE TABLE NYCTAXI.H_TRIP (
	trip_id						UUID NOT NULL
	, source					VARCHAR(128) NOT NULL
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(trip_id) ENABLED
)
ORDER BY trip_id, load_ts
SEGMENTED BY HASH(trip_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_H_TRIP AS
SELECT DISTINCT h.*
FROM NYCTAXI.H_TRIP h
INNER JOIN NYCTAXI.V_ODS_TRIP v
ON h.load_ts = v.load_ts;

-- Attributes for Trip record
DROP TABLE IF EXISTS NYCTAXI.S_TRIP_PICKUP;
CREATE TABLE NYCTAXI.S_TRIP_PICKUP (
	trip_id						UUID NOT NULL
	, tpep_pickup_datetime		DATETIME NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(trip_id) ENABLED
)
ORDER BY trip_id, load_ts
SEGMENTED BY HASH(trip_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_S_TRIP_PICKUP AS
SELECT DISTINCT s.*
FROM NYCTAXI.S_TRIP_PICKUP s
INNER JOIN NYCTAXI.V_ODS_TRIP v
ON s.load_ts = v.load_ts;


DROP TABLE IF EXISTS NYCTAXI.S_TRIP_DROPOFF;
CREATE TABLE NYCTAXI.S_TRIP_DROPOFF (
	trip_id						UUID NOT NULL
	, tpep_dropoff_datetime		DATETIME NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(trip_id) ENABLED
)
ORDER BY trip_id, load_ts
SEGMENTED BY HASH(trip_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_S_TRIP_DROPOFF AS
SELECT DISTINCT s.*
FROM NYCTAXI.S_TRIP_DROPOFF s
INNER JOIN NYCTAXI.V_ODS_TRIP v
ON s.load_ts = v.load_ts;


DROP TABLE IF EXISTS NYCTAXI.S_TRIP_PASS_COUNT;
CREATE TABLE NYCTAXI.S_TRIP_PASS_COUNT (
	trip_id						UUID NOT NULL
	, passenger_count			INTEGER NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(trip_id) ENABLED
)
ORDER BY trip_id, load_ts
SEGMENTED BY HASH(trip_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_S_TRIP_PASS_COUNT AS
SELECT DISTINCT s.*
FROM NYCTAXI.S_TRIP_PASS_COUNT s
INNER JOIN NYCTAXI.V_ODS_TRIP v
ON s.load_ts = v.load_ts;


DROP TABLE IF EXISTS NYCTAXI.S_TRIP_DISTANCE;
CREATE TABLE NYCTAXI.S_TRIP_DISTANCE	 (
	trip_id						UUID NOT NULL
	, trip_distance				FLOAT NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(trip_id) ENABLED
)
ORDER BY trip_id, load_ts
SEGMENTED BY HASH(trip_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_S_TRIP_DISTANCE AS
SELECT DISTINCT s.*
FROM NYCTAXI.S_TRIP_DISTANCE s
INNER JOIN NYCTAXI.V_ODS_TRIP v
ON s.load_ts = v.load_ts;


DROP TABLE IF EXISTS NYCTAXI.S_TRIP_FARE;
CREATE TABLE NYCTAXI.S_TRIP_FARE	 (
	trip_id						UUID NOT NULL
	, fare_amount				FLOAT NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(trip_id) ENABLED
)
ORDER BY trip_id, load_ts
SEGMENTED BY HASH(trip_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_S_TRIP_FARE AS
SELECT DISTINCT s.*
FROM NYCTAXI.S_TRIP_FARE s
INNER JOIN NYCTAXI.V_ODS_TRIP v
ON s.load_ts = v.load_ts;


DROP TABLE IF EXISTS NYCTAXI.S_TRIP_EXTRA;
CREATE TABLE NYCTAXI.S_TRIP_EXTRA	 (
	trip_id						UUID NOT NULL
	, extra						FLOAT NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(trip_id) ENABLED
)
ORDER BY trip_id, load_ts
SEGMENTED BY HASH(trip_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_S_TRIP_EXTRA AS
SELECT DISTINCT s.*
FROM NYCTAXI.S_TRIP_EXTRA s
INNER JOIN NYCTAXI.V_ODS_TRIP v
ON s.load_ts = v.load_ts;


DROP TABLE IF EXISTS NYCTAXI.S_TRIP_TIP;
CREATE TABLE NYCTAXI.S_TRIP_TIP	 (
	trip_id						UUID NOT NULL
	, tip_amount				FLOAT NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(trip_id) ENABLED
)
ORDER BY trip_id, load_ts
SEGMENTED BY HASH(trip_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_S_TRIP_TIP AS
SELECT DISTINCT s.*
FROM NYCTAXI.S_TRIP_TIP s
INNER JOIN NYCTAXI.V_ODS_TRIP v
ON s.load_ts = v.load_ts;


DROP TABLE IF EXISTS NYCTAXI.S_TRIP_TOLLS;
CREATE TABLE NYCTAXI.S_TRIP_TOLLS	 (
	trip_id						UUID NOT NULL
	, tolls_amount				FLOAT NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(trip_id) ENABLED
)
ORDER BY trip_id, load_ts
SEGMENTED BY HASH(trip_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_S_TRIP_TOLLS AS
SELECT DISTINCT s.*
FROM NYCTAXI.S_TRIP_TOLLS s
INNER JOIN NYCTAXI.V_ODS_TRIP v
ON s.load_ts = v.load_ts;


DROP TABLE IF EXISTS NYCTAXI.S_TRIP_TOTAL;
CREATE TABLE NYCTAXI.S_TRIP_TOTAL	 (
	trip_id						UUID NOT NULL
	, total_amount				FLOAT NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(trip_id) ENABLED
)
ORDER BY trip_id, load_ts
SEGMENTED BY HASH(trip_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_S_TRIP_TOTAL AS
SELECT DISTINCT s.*
FROM NYCTAXI.S_TRIP_TOTAL s
INNER JOIN NYCTAXI.V_ODS_TRIP v
ON s.load_ts = v.load_ts;



-- Anchor for Payment type
DROP TABLE IF EXISTS NYCTAXI.H_PAYMENT;
CREATE TABLE NYCTAXI.H_PAYMENT (
	payment_type_id				UUID NOT NULL
	, source					VARCHAR(128) NOT NULL
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(payment_type_id) ENABLED
)
ORDER BY payment_type_id, load_ts
SEGMENTED BY HASH(payment_type_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_H_PAYMENT AS
SELECT DISTINCT h.*
FROM NYCTAXI.H_PAYMENT h
INNER JOIN NYCTAXI.V_ODS_PAYMENT_TYPE v
ON h.load_ts = v.load_ts;

-- Attribute for Payment type
DROP TABLE IF EXISTS NYCTAXI.S_PAYMENT_ID;
CREATE TABLE NYCTAXI.S_PAYMENT_ID (
	payment_type_id				UUID NOT NULL
	, payment_id				INTEGER NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(payment_type_id) ENABLED
)
ORDER BY payment_type_id, load_ts
SEGMENTED BY HASH(payment_type_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_S_PAYMENT_ID AS
SELECT DISTINCT s.*
FROM NYCTAXI.S_PAYMENT_ID s
INNER JOIN NYCTAXI.V_ODS_PAYMENT_TYPE v
ON s.load_ts = v.load_ts;

DROP TABLE IF EXISTS NYCTAXI.S_PAYMENT_TYPE;
CREATE TABLE NYCTAXI.S_PAYMENT_TYPE (
	payment_type_id				UUID NOT NULL
	, type						VARCHAR(32) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(payment_type_id) ENABLED
)
ORDER BY payment_type_id, load_ts
SEGMENTED BY HASH(payment_type_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_S_PAYMENT_TYPE AS
SELECT DISTINCT s.*
FROM NYCTAXI.S_PAYMENT_TYPE s
INNER JOIN NYCTAXI.V_ODS_PAYMENT_TYPE v
ON s.load_ts = v.load_ts;


-- Tie Trip -- Payment Type
DROP TABLE IF EXISTS NYCTAXI.L_TRIP_PAYMENT_TYPE;
CREATE TABLE NYCTAXI.L_TRIP_PAYMENT_TYPE (
	trip_id						UUID NOT NULL
	, payment_type_id			UUID NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(trip_id, payment_type_id) ENABLED
)
ORDER BY trip_id, payment_type_id, load_ts
SEGMENTED BY HASH(trip_id, payment_type_id) ALL NODES
;


-- Anchor for Location
DROP TABLE IF EXISTS NYCTAXI.H_LOCATION;
CREATE TABLE NYCTAXI.H_LOCATION (
	loc_id						UUID NOT NULL
	, source					VARCHAR(128) NOT NULL
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(loc_id) ENABLED
)
ORDER BY loc_id, load_ts
SEGMENTED BY HASH(loc_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_H_LOCATION AS
SELECT DISTINCT h.*
FROM NYCTAXI.H_LOCATION h
INNER JOIN NYCTAXI.V_ODS_ZONES v
ON h.load_ts = v.load_ts;


-- Attribute for Location
DROP TABLE IF EXISTS NYCTAXI.S_LOCATION_ID;
CREATE TABLE NYCTAXI.S_LOCATION_ID (
	loc_id						UUID NOT NULL
	, locationid				INTEGER NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(loc_id) ENABLED
)
ORDER BY loc_id, load_ts
SEGMENTED BY HASH(loc_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_S_LOCATION_ID AS
SELECT DISTINCT s.*
FROM NYCTAXI.S_LOCATION_ID s
INNER JOIN NYCTAXI.V_ODS_ZONES v
ON s.load_ts = v.load_ts;


DROP TABLE IF EXISTS NYCTAXI.S_LOCATION_BOROUGH;
CREATE TABLE NYCTAXI.S_LOCATION_BOROUGH (
	loc_id						UUID NOT NULL
	, borough					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(loc_id) ENABLED
)
ORDER BY loc_id, load_ts
SEGMENTED BY HASH(loc_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_S_LOCATION_BOROUGH AS
SELECT DISTINCT s.*
FROM NYCTAXI.S_LOCATION_BOROUGH s
INNER JOIN NYCTAXI.V_ODS_ZONES v
ON s.load_ts = v.load_ts;


DROP TABLE IF EXISTS NYCTAXI.S_LOCATION_ZONE;
CREATE TABLE NYCTAXI.S_LOCATION_ZONE (
	loc_id						UUID NOT NULL
	, zone						VARCHAR(128) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(loc_id) ENABLED
)
ORDER BY loc_id, load_ts
SEGMENTED BY HASH(loc_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_S_LOCATION_ZONE AS
SELECT DISTINCT s.*
FROM NYCTAXI.S_LOCATION_ZONE s
INNER JOIN NYCTAXI.V_ODS_ZONES v
ON s.load_ts = v.load_ts;


DROP TABLE IF EXISTS NYCTAXI.S_LOCATION_SERVICE_ZONE;
CREATE TABLE NYCTAXI.S_LOCATION_SERVICE_ZONE (
	loc_id						UUID NOT NULL
	, service_zone				VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(loc_id) ENABLED
)
ORDER BY loc_id, load_ts
SEGMENTED BY HASH(loc_id) ALL NODES
;

CREATE OR REPLACE VIEW NYCTAXI.V_S_LOCATION_SERVICE_ZONE AS
SELECT DISTINCT s.*
FROM NYCTAXI.S_LOCATION_SERVICE_ZONE s
INNER JOIN NYCTAXI.V_ODS_ZONES v
ON s.load_ts = v.load_ts;



-- Tie Trip -- Location Pickup
DROP TABLE IF EXISTS NYCTAXI.L_TRIP_PU_LOCATION;
CREATE TABLE NYCTAXI.L_TRIP_PU_LOCATION (
	trip_id						UUID NOT NULL
	, loc_id					UUID NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(trip_id, loc_id) ENABLED
)
ORDER BY trip_id, loc_id, load_ts
SEGMENTED BY HASH(trip_id, loc_id) ALL NODES
;

-- Tie Trip -- Location Dropoff
DROP TABLE IF EXISTS NYCTAXI.L_TRIP_DO_LOCATION;
CREATE TABLE NYCTAXI.L_TRIP_DO_LOCATION (
	trip_id						UUID NOT NULL
	, loc_id					UUID NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY(trip_id, loc_id) ENABLED
)
ORDER BY trip_id, loc_id, load_ts
SEGMENTED BY HASH(trip_id, loc_id) ALL NODES
;


-- Data Marts
-- Hourly TS for number of pickup in zones
CREATE OR REPLACE VIEW NYCTAXI.V_DM_HOURLY_TS_ZONE AS
SELECT count(*)
	, DATE_PART('ISOYEAR', p.tpep_pickup_datetime) AS year
	, DATE_PART('MONTH', p.tpep_pickup_datetime) AS month
	, DATE_PART('DAY', p.tpep_pickup_datetime) AS day
	, DATE_PART('HOUR', p.tpep_pickup_datetime) AS hour
	, z.zone
FROM NYCTAXI.V_H_TRIP t
INNER JOIN NYCTAXI.V_S_TRIP_PICKUP p
ON t.trip_id = p.trip_id
INNER JOIN NYCTAXI.L_TRIP_PU_LOCATION l
ON t.trip_id = l.trip_id
INNER JOIN NYCTAXI.V_S_LOCATION_ZONE z
ON l.loc_id = z.loc_id
GROUP BY year, month, day, hour, zone
ORDER BY year, month, day, hour, zone;

-- Hourly number of pickup in zones
CREATE OR REPLACE VIEW NYCTAXI.V_DM_HOURLY_ZONE AS
SELECT count(*)
	, DATE_PART('HOUR', p.tpep_pickup_datetime) AS hour
	, z.zone
FROM NYCTAXI.V_H_TRIP t
INNER JOIN NYCTAXI.V_S_TRIP_PICKUP p
ON t.trip_id = p.trip_id
INNER JOIN NYCTAXI.L_TRIP_PU_LOCATION l
ON t.trip_id = l.trip_id
INNER JOIN NYCTAXI.V_S_LOCATION_ZONE z
ON l.loc_id = z.loc_id
GROUP BY hour, zone
ORDER BY hour, zone;

-- Day of week number of pickup in zones
CREATE OR REPLACE VIEW NYCTAXI.V_DM_DOW_ZONE AS
SELECT count(*)
	, DATE_PART('DOW', p.tpep_pickup_datetime) AS dow
	, z.zone
FROM NYCTAXI.V_H_TRIP t
INNER JOIN NYCTAXI.V_S_TRIP_PICKUP p
ON t.trip_id = p.trip_id
INNER JOIN NYCTAXI.L_TRIP_PU_LOCATION l
ON t.trip_id = l.trip_id
INNER JOIN NYCTAXI.V_S_LOCATION_ZONE z
ON l.loc_id = z.loc_id
GROUP BY dow, zone
ORDER BY dow, zone;

-- Day of week Average trip amount in zones
CREATE OR REPLACE VIEW NYCTAXI.V_DM_DOW_AVG_ZONE AS
SELECT avg(a.total_amount)
	, DATE_PART('DOW', p.tpep_pickup_datetime) AS dow
	, z.zone
FROM NYCTAXI.V_H_TRIP t
INNER JOIN NYCTAXI.V_S_TRIP_PICKUP p
ON t.trip_id = p.trip_id
INNER JOIN NYCTAXI.L_TRIP_PU_LOCATION l
ON t.trip_id = l.trip_id
INNER JOIN NYCTAXI.V_S_LOCATION_ZONE z
ON l.loc_id = z.loc_id
INNER JOIN NYCTAXI.V_S_TRIP_TOTAL a
ON t.trip_id = a.trip_id
GROUP BY dow, zone
ORDER BY dow, zone;

-- Hourly Average trip amount in zones
CREATE OR REPLACE VIEW NYCTAXI.V_DM_HOURLY_AVG_ZONE AS
SELECT avg(a.total_amount)
	, DATE_PART('HOUR', p.tpep_pickup_datetime) AS hour
	, z.zone
FROM NYCTAXI.V_H_TRIP t
INNER JOIN NYCTAXI.V_S_TRIP_PICKUP p
ON t.trip_id = p.trip_id
INNER JOIN NYCTAXI.L_TRIP_PU_LOCATION l
ON t.trip_id = l.trip_id
INNER JOIN NYCTAXI.V_S_LOCATION_ZONE z
ON l.loc_id = z.loc_id
INNER JOIN NYCTAXI.V_S_TRIP_TOTAL a
ON t.trip_id = a.trip_id
GROUP BY hour, zone
ORDER BY hour, zone;
