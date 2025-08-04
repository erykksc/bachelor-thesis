#!/bin/bash

set -euo pipefail 

# Default values
DB_TARGET="cratedb"
BATCH_SIZE=2000
DB_CONN_STR='postgresql://crate:crate@localhost:5432'
NWORKERS=10
NCOMPLEX_QUERIES=100000000000 # 100 billion queries, it should be impossible to perform so that the timeout is reached
NSIMPLE_QUERIES=100000000000 # 100 billion queries
TRIPS='../dataset-generator/output/escooter-trips-large.csv'
QRS_TIMEOUT='25m'

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --db-target) DB_TARGET="$2"; shift ;;
    --db-conn) DB_CONN_STR="$2"; shift ;;
    --batch-size) BATCH_SIZE="$2"; shift ;;
    --nworkers) NWORKERS="$2"; shift ;;
    --trips) TRIPS="$2"; shift ;;
    --queries-timeout) QRS_TIMEOUT="$2"; shift ;;
    *) echo "Unknown option $1"; exit 1 ;;
  esac
  shift
done

# Init DB
go run . --mode init \
  --dbTarget $DB_TARGET \
  --db $DB_CONN_STR \
  --migrations "./migrations/$DB_TARGET"

# Insert
go run . --mode insert \
  --dbTarget $DB_TARGET \
  --db $DB_CONN_STR \
  --nworkers $NWORKERS \
  --batch-size $BATCH_SIZE \
  --bulk-insert \
  --trips $TRIPS

sleep 180

# Simple queries
timeout --signal=INT $QRS_TIMEOUT go run . --mode query \
  --dbTarget $DB_TARGET \
  --db $DB_CONN_STR \
  --nworkers $NWORKERS \
  --queries "./schemas/$DB_TARGET-simple-read-queries.tmpl" \
  --nqueries $NSIMPLE_QUERIES

sleep 180

# Complex queries
timeout --signal=INT $QRS_TIMEOUT go run . --mode query \
  --dbTarget $DB_TARGET \
  --db $DB_CONN_STR \
  --nworkers $NWORKERS \
  --queries "./schemas/$DB_TARGET-complex-read-queries.tmpl" \
  --nqueries $NCOMPLEX_QUERIES
