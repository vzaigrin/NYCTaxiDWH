#!/bin/sh

TR=/usr/bin/tr
AWK=/usr/bin/awk
INBOX=../data/trips/inbox
ARCHIVE=../data/trips/archive

VSQL=/opt/vertica/bin/vsql
SERVER=localhost
USER=dbadmin

trips=($INBOX/*.csv)
if [ -n "${trips[1]}" ]
then
  for i in $INBOX/*.csv
  do
    FILE=`basename $i`
    echo $FILE
    $TR -d '\015' < $i | $AWK -v FILE=$FILE -v TS="`date "+%Y-%m-%d %H:%M:%S"`" 'BEGIN {OFS=""} { if (NR > 1) print $0, ",TLC,", FILE, ",", TS }' | $VSQL -h$SERVER -U$USER -c "COPY NYCTAXI.STG_TRIP FROM STDIN DELIMITER ',' DIRECT"
    mv $i $ARCHIVE
  done
  $VSQL -h$SERVER -U$USER -f load_trips.sql
fi


