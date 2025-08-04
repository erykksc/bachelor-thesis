#!/bin/bash

set -euo pipefail 

# Default values
BATCH_SIZE=2000
DB='postgresql://postgres:postgres@localhost:5432'
NWORKERS=16
NCOMPLEXQRS=100000000000 # 100 billion queries, it should be impossible to perform so that the timeout is reached
NSIMPLEQRS=100000000000 # 100 billion queries
TRIPS='../dataset-generator/output/escooter-trips-small.csv'
QRS_TIMEOUT='25m'

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --db) DB="$2"; shift ;;
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
  --dbTarget mobilitydbc \
  --db $DB \
  --migrations ./migrations/mobilitydbc

# Insert
go run . --mode insert \
  --dbTarget mobilitydbc \
  --db $DB \
  --nworkers $NWORKERS \
  --batch-size $BATCH_SIZE \
  --bulk-insert \
  --trips $TRIPS

# Simple queries
timeout --signal=INT $QRS_TIMEOUT go run . --mode query \
  --dbTarget mobilitydbc \
  --db $DB \
  --nworkers $NWORKERS \
  --queries ./schemas/mobilitydb-simple-read-queries.tmpl \
  --nqueries $NSIMPLEQRS

# Complex queries
timeout --signal=INT $QRS_TIMEOUT go run . --mode query \
  --dbTarget mobilitydbc \
  --db $DB \
  --nworkers $NWORKERS \
  --queries ./schemas/mobilitydb-complex-read-queries.tmpl \
  --nqueries $NCOMPLEXQRS
