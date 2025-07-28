package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func benchmarkInserts(ctx context.Context, connString string, numWorkers int, batchSize int, dbTarget DBTarget, tripsFilename string) {
	logger.Info("Starting Insert Benchmark", "dbConnString", connString, "numWorkers", numWorkers, "dbTarget", dbTarget, "tripsFilename", tripsFilename)
	// create specified number of workers
	var wg sync.WaitGroup
	jobs := make(chan []TripEvent, numWorkers*5) // batches of events
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			insertWorker(ctx, id, jobs, connString, dbTarget)
			wg.Done()
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

	// read the trips csv and send batches to workers
	startTime := time.Now()
	tripEventsCount := 0
	batch := make([]TripEvent, 0, batchSize)

csvScanLoop:
	for {
		rec, err := r.Read()
		if err == io.EOF {
			// Send remaining batch if not empty
			if len(batch) > 0 {
				select {
				case <-ctx.Done():
					break csvScanLoop
				case jobs <- batch:
				}
			}
			break
		} else if err != nil {
			logger.Error("Error in read of trips csv", "error", err)
			os.Exit(1)
		}

		tripEvent := TripEvent{
			EventID:   rec[0],
			TripID:    rec[1],
			Timestamp: rec[2],
			Latitude:  rec[3],
			Longitude: rec[4],
		}

		batch = append(batch, tripEvent)
		tripEventsCount++

		// Send batch when full
		if len(batch) >= batchSize {
			select {
			case <-ctx.Done():
				break csvScanLoop
			case jobs <- batch:
			}
			batch = make([]TripEvent, 0, batchSize)
		}

		if tripEventsCount%10000 == 0 {
			logger.Info("Insert progress", "totalInsertedToJobQueue", tripEventsCount, "timeElapsedInSec", time.Since(startTime).Seconds())
		}
	}

	close(jobs)
	wg.Wait()
	if ctx.Err() == nil {
		logger.Info("All escooter trip events added", "count", tripEventsCount, "timeElapsedInSec", time.Since(startTime).Seconds())
	}

}

// each worker should measure and log all available metrics
//   - whether the insert was sucessful
//   - the time it took to insert (if provided in the response)
//   - the latency of getting a response
//   - time spend waiting for receiving the next job through channel
func insertWorker(ctx context.Context, id int, tripEventBatches <-chan []TripEvent, connString string, dbTarget DBTarget) {
	logger.Info("Worker started", "id", id)

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		logger.Error("Unable to connect to database", "error", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)
	logger.Info("Worker connected to db", "id", id)

	getInsertTripEventSql := getInsertTripEventCratedbSql
	switch dbTarget {
	case CrateDB:
		getInsertTripEventSql = getInsertTripEventCratedbSql
	case MobilityDB:
		getInsertTripEventSql = getInsertTripEventMobilitydbSql
	}

	insertedEvents := 0
	failedInserts := 0

	defer func() {
		logger.Info(
			"Insert worker finished",
			"id", id,
			"insertedEvents", insertedEvents,
			"failedInserts", failedInserts,
			"ctxErr", ctx.Err(),
		)
	}()

	lastJobFinishTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			logger.Info("Worker finished because the passed context is marked as done", "id", id)
			return
		case batch, ok := <-tripEventBatches:
			if !ok {
				logger.Info("Worker finished", "id", id)
				return
			}

			logger.Debug("Worker: batch received, inserting into db...", "id", id, "batchSize", len(batch))

			waitedForJobTime := time.Since(lastJobFinishTime)

			successfullInserts := len(batch)
			startTime := time.Now()

			// Use pgx batch for efficient batch inserts
			pgxBatch := &pgx.Batch{}
			for _, tEvent := range batch {
				query := getInsertTripEventSql(tEvent)
				pgxBatch.Queue(query)
			}

			batchResults := conn.SendBatch(ctx, pgxBatch)
			eventsInBatch := len(batch)
			for range eventsInBatch {
				_, err := batchResults.Exec()
				if err != nil {
					successfullInserts--
					failedInserts++
					logger.Debug("Error inserting escooter event", "worker", id, "error", err)
				} else {
					insertedEvents++
				}
			}
			batchResults.Close()

			endTime := time.Now()
			logger.Info("Worker finished batch insert",
				"workerId", id,
				"jobType", "batch_insert",
				"batchSize", eventsInBatch,
				"startTime", startTime,
				"endTime", endTime,
				"insertTimeInMs", endTime.Sub(startTime).Milliseconds(),
				"waitedForJobTimeInMs", waitedForJobTime.Milliseconds(),
				"successfullyInserted", successfullInserts,
			)
			lastJobFinishTime = time.Now()

			logger.Debug("Worker: batch inserted into db", "id", id, "batchSize", len(batch))
		}
	}
}

func getInsertTripEventCratedbSql(tEvent TripEvent) string {
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

func getInsertTripEventMobilitydbSql(tEvent TripEvent) string {
	return fmt.Sprintf(`
	INSERT INTO escooter_events (
		event_id,
		trip_id,
		timestamp,
		location
	)
	VALUES (
		'%s', 
		'%s',
		'%s',
		tgeompoint 'Point(%s %s)@%s'
	);`, tEvent.EventID, tEvent.TripID, tEvent.Timestamp, tEvent.Longitude, tEvent.Latitude, tEvent.Timestamp)
}
