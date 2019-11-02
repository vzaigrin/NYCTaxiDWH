#!/bin/sh

TR=/usr/bin/tr
AWK=/usr/bin/awk
DATA=../data/payment_type.csv
FILE=`basename $DATA`

VSQL=/opt/vertica/bin/vsql
SERVER=localhost
USER=dbadmin

$TR -d '\015' < $DATA | $AWK -v FILE=$FILE -v TS="`date "+%Y-%m-%d %H:%M:%S"`" 'BEGIN {OFS=""} { if (NR > 1) print $0, ",TLC,", FILE, ",", TS }' | $VSQL -h$SERVER -U$USER -c "COPY NYCTAXI.STG_PAYMENT_TYPE FROM STDIN DELIMITER ',' DIRECT"

$VSQL -h$SERVER -U$USER -f load_payment_type.sql

