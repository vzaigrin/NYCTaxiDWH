INSERT INTO NYCTAXI.ODS_PAYMENT_TYPE
SELECT DISTINCT
    uuid_generate() AS payment_type_id
    , type_id
    , type
    , source
    , filename
    , load_ts
FROM NYCTAXI.STG_PAYMENT_TYPE;

INSERT INTO NYCTAXI.H_PAYMENT
SELECT payment_type_id, source, filename, load_ts
FROM NYCTAXI.V_ODS_PAYMENT_TYPE;

INSERT INTO NYCTAXI.S_PAYMENT_ID
SELECT payment_type_id, type_id, load_ts
FROM NYCTAXI.V_ODS_PAYMENT_TYPE;

INSERT INTO NYCTAXI.S_PAYMENT_TYPE
SELECT payment_type_id, type, load_ts
FROM NYCTAXI.V_ODS_PAYMENT_TYPE
WHERE type IS NOT NULL;

COMMIT;
