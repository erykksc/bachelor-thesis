#!/bin/bash

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

