INSERT INTO NYCTAXI.ODS_ZONES
SELECT DISTINCT
    uuid_generate() AS loc_id
    , LocationID
    , REPLACE(Borough, '"', '') AS Borough
    , REPLACE(Zone, '"', '') AS Zone
    , REPLACE(service_zone, '"', '') AS service_zone
    , source
    , filename
    , load_ts
FROM NYCTAXI.STG_ZONES;

INSERT INTO NYCTAXI.H_LOCATION
SELECT loc_id, source, filename, load_ts
FROM NYCTAXI.V_ODS_ZONES;

INSERT INTO NYCTAXI.S_LOCATION_ID
SELECT loc_id, locationid, load_ts
FROM NYCTAXI.V_ODS_ZONES;

INSERT INTO NYCTAXI.S_LOCATION_BOROUGH
SELECT loc_id, borough, load_ts
FROM NYCTAXI.V_ODS_ZONES
WHERE borough IS NOT NULL;

INSERT INTO NYCTAXI.S_LOCATION_ZONE
SELECT loc_id, zone, load_ts
FROM NYCTAXI.V_ODS_ZONES
WHERE zone IS NOT NULL;

INSERT INTO NYCTAXI.S_LOCATION_SERVICE_ZONE
SELECT loc_id, service_zone, load_ts
FROM NYCTAXI.V_ODS_ZONES
WHERE service_zone IS NOT NULL;

COMMIT;
