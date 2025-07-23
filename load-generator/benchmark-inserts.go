package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func benchmarkInserts(ctx context.Context, connString string, numWorkers int, dbTarget DBTarget, tripsFilename string) {
	logger.Info("Starting Insert Benchmark", "dbConnString", connString, "numWorkers", numWorkers, "dbTarget", dbTarget, "tripsFilename", tripsFilename)
	// create specified number of workers
	var wg sync.WaitGroup
	jobs := make(chan *TripEvent, runtime.NumCPU()*4) // larger buffer to combat workers waiting for main thread to read the csv file
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			insertWorker(ctx, id, jobs, &wg, connString, dbTarget)
		}(i)
	}
	logger.Info("Started worker threads", "numWorkers", numWorkers)

	// open the csv file
	f, err := os.Open(tripsFilename)
	if err != nil {
		logger.Error("Error opening file", "error", err, "filename", tripsFilename)
		os.Exit(1)
	}
	defer f.Close()
	r := csv.NewReader(f)

	// read header of csv
	if _, err := r.Read(); err != nil {
		logger.Error("Error in read pois header", "error", err)
		os.Exit(1)
	}

	// read the trips csv and send the jobs to workers
	startTime := time.Now()
	tripEventsCount := 0
csvScanLoop:
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			logger.Error("Error in read of trips csv", "error", err)
			os.Exit(1)
		}

		tripEvent := &TripEvent{
			EventID:   rec[0],
			TripID:    rec[1],
			Timestamp: rec[2],
			Latitude:  rec[3],
			Longitude: rec[4],
		}

		select {
		case <-ctx.Done():
			break csvScanLoop
		case jobs <- tripEvent:
		}
		tripEventsCount++
	}

	close(jobs)
	wg.Wait()
	if ctx.Err() == nil {
		logger.Info("All escooter trip events added", "count", tripEventsCount, "timeElapsed", time.Since(startTime))
	}

}

// each worker should measure and log all available metrics
//   - whether the insert was sucessful
//   - the time it took to insert (if provided in the response)
//   - the latency of getting a response
//   - time spend waiting for receiving the next job through channel
func insertWorker(ctx context.Context, id int, tripEvents <-chan *TripEvent, wg *sync.WaitGroup, connString string, dbTarget DBTarget) {
	defer wg.Done()

	logger.Info("Worker started", "id", id)

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		logger.Error("Unable to connect to database", "error", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)
	logger.Info("Worker conntected to db", "id", id)

	getInsertTripEventSql := getInsertTripEventCratedbSql
	switch dbTarget {
	case CrateDB:
		getInsertTripEventSql = getInsertTripEventCratedbSql
	case MobilityDB:
		getInsertTripEventSql = getInsertTripEventMobilitydbSql
	}

	lastJobFinishTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			logger.Info("Worker finished because the passed context is marked as done", "id", id)
			return
		case tEvent, ok := <-tripEvents:
			if !ok {
				logger.Info("Worker finished", "id", id)
				return
			}

			logger.Debug("Worker: tripEvent received, inserting into db...", "id", id, "eventId", tEvent.EventID)

			waitedForJobTime := time.Since(lastJobFinishTime)

			query := getInsertTripEventSql(tEvent)

			querySuccessful := true
			startTime := time.Now()
			cmdTag, err := conn.Exec(ctx, query)
			if err != nil {
				querySuccessful = false
			}

			endTime := time.Now()
			logger.Info("Worker finished insert",
				"workerId", id,
				"jobType", "insert",
				"insertTime", endTime.Sub(startTime),
				"waitedForJobTime", waitedForJobTime,
				"successful", querySuccessful,
				"cmdTag", cmdTag,
				"queryErr", err,
			)
			lastJobFinishTime = time.Now()

			logger.Debug("Worker: tripEvent inserted into db", "id", id, "eventId", tEvent.EventID)
		}
	}
}

func getInsertTripEventCratedbSql(tEvent *TripEvent) string {
	return fmt.Sprintf(`
	INSERT INTO escooter_events (
		event_id,
		trip_id,
		timestamp,
		geo_point
	)
	VALUES (
		'%s', '%s', '%s', [%s, %s]
	);`, tEvent.EventID, tEvent.TripID, tEvent.Timestamp, tEvent.Latitude, tEvent.Longitude)
}

func getInsertTripEventMobilitydbSql(tEvent *TripEvent) string {
	return fmt.Sprintf(`
	INSERT INTO escooter_events (
		event_id,
		trip_id,
		timestamp,
		location
	)
	VALUES (
		%s, 
		%s,
		%s,
		tgeompoint 'Point(%s %s)@%s'
	);`, tEvent.EventID, tEvent.TripID, tEvent.Timestamp, tEvent.Longitude, tEvent.Latitude, tEvent.Timestamp)
}
