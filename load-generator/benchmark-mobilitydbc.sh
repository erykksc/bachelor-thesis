#!/bin/bash

# ALL OF THOSE BENCHMARKS NEED TO BE RUN ON DIFFERENT CLUSTER SIZES
# ALL OF THOSE BENCHMARKS NEED TO BE RUN BOTH FOR CRATEDB AND MOBILITYDBC

# INSERTS
# run different amount of client threads 10,100,1000
	# run different amount of inserts per batch 100,1000,10000
		# run inserts using --bulk-insert and without (this is a very important finding)
		# without bulk insert, use only a small dataset

# analyze the results and find the fastest way to insert


# SIMPLE QUERIES
# run different amount of client threads 10,100,1000
	# run different amount of queries per batch/request 1,100,1000
		# insert the data as fast as possible

# COMPLEX QUERIES
# run different amount of client threads 10,100,1000
	# insert the data as fast as possible

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
