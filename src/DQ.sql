-- Data Quality
-- Check that all Payment records from ODS are in DDS
SELECT *
FROM NYCTAXI.ODS_PAYMENT_TYPE o
LEFT JOIN NYCTAXI.H_PAYMENT h
ON o.payment_type_id = h.payment_type_id
WHERE h.payment_type_id IS NULL
LIMIT 1 OVER (PARTITION BY o.type_id ORDER BY o.load_ts DESC);

-- Check that all Zones (Locations) from ODS are in DDS
SELECT *
FROM NYCTAXI.ODS_ZONES o
LEFT JOIN NYCTAXI.H_LOCATION h
ON o.loc_id = h.loc_id
WHERE h.loc_id IS NULL
LIMIT 1 OVER (PARTITION BY o.LocationID ORDER BY o.load_ts DESC);

-- Check that all Trips from ODS are in DDS
SELECT *
FROM NYCTAXI.ODS_TRIP o
LEFT JOIN NYCTAXI.H_TRIP h
ON o.trip_id = h.trip_id
WHERE h.trip_id IS NULL
LIMIT 1 OVER (PARTITION BY o.tpep_pickup_datetime, o.tpep_dropoff_datetime, o.PULocationID, o.DOLocationID ORDER BY o.load_ts DESC);
