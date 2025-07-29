#!/bin/bash

# $1 - postgresql connection string
# $2 - escooter_trips csv file

psql $1 -c "\copy your_table(event_id, trip_id, timestamp, latitude, longitude) FROM '$2' WITH (FORMAT csv, HEADER)"
