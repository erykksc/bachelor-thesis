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

type InsertEvent struct {
	WorkerID             int
	JobType              string
	BatchSize            int
	UseBulkInsert        bool
	StartTime            string
	EndTime              string
	InsertDurationMs     int64
	WaitedForJobTimeMs   int64
	SuccessfullyInserted int
	FailedInserts        int
}

func benchmarkInserts(ctx context.Context, connString string, numWorkers int, batchSize int, useBulkInsert bool, dbTarget DBTarget, tripsFilename string, csvWriter *csv.Writer) {
	logger.Info("Starting Insert Benchmark", "dbConnString", connString, "numWorkers", numWorkers, "dbTarget", dbTarget.String(), "tripsFilename", tripsFilename)
	// create specified number of workers
	var wg sync.WaitGroup
	readyStatus := make(chan int, numWorkers)
	jobs := make(chan []TripEvent, numWorkers*5) // batches of events
	successCh := make(chan int, numWorkers)
	failureCh := make(chan int, numWorkers)
	eventCh := make(chan InsertEvent, numWorkers*10)
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			insertWorker(ctx, id, jobs, connString, dbTarget, useBulkInsert, successCh, failureCh, eventCh, readyStatus)
			wg.Done()
		}(i)
	}
	logger.Info("Started worker threads", "numWorkers", numWorkers)

	// Write CSV header
	csvHeader := []string{"workerId", "jobType", "batchSize", "useBulkInsert", "startTime", "endTime", "insertDurationMs", "waitedForJobTimeMs", "successfullyInserted", "failedInserts"}
	if err := csvWriter.Write(csvHeader); err != nil {
		logger.Error("Failed to write CSV header", "error", err)
		os.Exit(1)
	}

	// Start CSV writer goroutine
	var csvWg sync.WaitGroup
	csvWg.Add(1)
	go func() {
		defer csvWg.Done()
		for event := range eventCh {
			// Log the event (replacing worker logging)
			logger.Info("Worker finished batch insert",
				"workerId", event.WorkerID,
				"jobType", event.JobType,
				"batchSize", event.BatchSize,
				"useBulkInsert", event.UseBulkInsert,
				"startTime", event.StartTime,
				"endTime", event.EndTime,
				"insertDurationMs", event.InsertDurationMs,
				"waitedForJobTimeMs", event.WaitedForJobTimeMs,
				"successfullyInserted", event.SuccessfullyInserted,
			)

			// Write to CSV
			record := []string{
				fmt.Sprintf("%d", event.WorkerID),
				event.JobType,
				fmt.Sprintf("%d", event.BatchSize),
				fmt.Sprintf("%t", event.UseBulkInsert),
				event.StartTime,
				event.EndTime,
				fmt.Sprintf("%d", event.InsertDurationMs),
				fmt.Sprintf("%d", event.WaitedForJobTimeMs),
				fmt.Sprintf("%d", event.SuccessfullyInserted),
				fmt.Sprintf("%d", event.FailedInserts),
			}
			if err := csvWriter.Write(record); err != nil {
				logger.Error("Failed to write CSV record", "error", err)
			}
		}
	}()

	// Wait for all workers to signal ready
	workersReady := 0
Waiting4Workers:
	for {
		select {
		case <-ctx.Done():
			return
		case readyWorkerId := <-readyStatus:
			logger.Info("Worker reported ready", "id", readyWorkerId)
			workersReady += 1
			if workersReady == numWorkers {
				break Waiting4Workers
			}
		}
	}

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

	// Close event channel and wait for CSV writer to finish
	close(eventCh)
	csvWg.Wait()

	// Collect success and failure counts from all workers
	totalSuccesses := 0
	totalFailures := 0
	for range numWorkers {
		totalSuccesses += <-successCh
		totalFailures += <-failureCh
	}
	close(successCh)
	close(failureCh)

	endTime := time.Now()
	if ctx.Err() == nil {
		logger.Info("All escooter trip events added", "count", tripEventsCount, "timeElapsedInSec", endTime.Sub(startTime).Seconds(), "startTime", startTime, "endTime", endTime, "totalSuccesses", totalSuccesses, "totalFailures", totalFailures)
	}

	// Create trips table
	if dbTarget == MobilityDB {
		err := importEventsIntoTrips(ctx, connString)
		if err != nil {
			logger.Error("Error during import of events into trips table", "error", err)
			os.Exit(1)
		}
	}
}

// each worker should measure and log all available metrics
//   - whether the insert was sucessful
//   - the time it took to insert (if provided in the response)
//   - the latency of getting a response
//   - time spend waiting for receiving the next job through channel
func insertWorker(ctx context.Context, id int, tripEventBatches <-chan []TripEvent, connString string, dbTarget DBTarget, useBulkInsert bool, successCh chan<- int, failureCh chan<- int, eventCh chan<- InsertEvent, readyStatus chan<- int) {
	logger.Info("Worker started", "id", id)

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		logger.Error("Unable to connect to database", "error", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)
	logger.Info("Worker connected to db", "id", id)

	readyStatus <- id

	insertEventSql := insertEventCratedbSql
	switch dbTarget {
	case CrateDB:
		insertEventSql = insertEventCratedbSql
	case MobilityDB:
		insertEventSql = insertEventMobilitydbSql
	}

	bulkInsertEventSql := bulkInsertEventCratedbSql
	switch dbTarget {
	case CrateDB:
		bulkInsertEventSql = bulkInsertEventCratedbSql
	case MobilityDB:
		bulkInsertEventSql = bulkInsertEventMobilitydbSql
	}

	insertedByWorker := 0
	failedInsertsByWorker := 0

	defer func() {
		successCh <- insertedByWorker
		failureCh <- failedInsertsByWorker
		logger.Info(
			"Insert worker finished",
			"id", id,
			"insertedEvents", insertedByWorker,
			"failedInserts", failedInsertsByWorker,
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

			insertedInQuery := 0
			batchSize := len(batch)
			startTime := time.Now()

			if useBulkInsert {
				insertQuery := bulkInsertEventSql(batch)
				res, err := conn.Exec(ctx, insertQuery)
				insertedInQuery += int(res.RowsAffected())
				logger.Info("Bulk inserted trip events", "worker", id, "rowsAffected", res.RowsAffected(), "error", err)
			} else {
				// Use pgx batch for efficient batch inserts
				pgxBatch := &pgx.Batch{}
				for _, tEvent := range batch {
					query := insertEventSql(tEvent)
					pgxBatch.Queue(query)
				}

				batchResults := conn.SendBatch(ctx, pgxBatch)
				for range batchSize {
					_, err := batchResults.Exec()
					if err != nil {
						logger.Error("Error inserting escooter event", "worker", id, "error", err)
					} else {
						insertedInQuery++
					}
				}
				batchResults.Close()
			}

			endTime := time.Now()

			// Send event to main thread for logging and CSV writing
			event := InsertEvent{
				WorkerID:             id,
				JobType:              "batch_insert",
				BatchSize:            batchSize,
				UseBulkInsert:        useBulkInsert,
				StartTime:            startTime.Format(time.RFC3339),
				EndTime:              endTime.Format(time.RFC3339),
				InsertDurationMs:     endTime.Sub(startTime).Milliseconds(),
				WaitedForJobTimeMs:   waitedForJobTime.Milliseconds(),
				SuccessfullyInserted: insertedInQuery,
				FailedInserts:        batchSize - insertedInQuery,
			}
			eventCh <- event

			insertedByWorker += insertedInQuery
			failedInsertsByWorker += batchSize - insertedInQuery

			lastJobFinishTime = time.Now()
		}
	}
}

func insertEventCratedbSql(tEvent TripEvent) string {
	return fmt.Sprintf(`
INSERT INTO escooter_events (
	event_id, trip_id, timestamp, geo_point
)
VALUES (
	'%s', '%s', '%s', [%s, %s]
);`, tEvent.EventID, tEvent.TripID, tEvent.Timestamp, tEvent.Longitude, tEvent.Latitude)
}

func insertEventMobilitydbSql(tEvent TripEvent) string {
	return fmt.Sprintf(`
INSERT INTO escooter_events (
	event_id, trip_id, timestamp, geo_point
)
VALUES (
	'%s', '%s', '%s', 'SRID=4326;POINT(%s %s)'
);`, tEvent.EventID, tEvent.TripID, tEvent.Timestamp, tEvent.Longitude, tEvent.Latitude)
}

func bulkInsertEventCratedbSql(events []TripEvent) string {
	eventIds := make([]string, len(events))
	tripIds := make([]string, len(events))
	timestamps := make([]string, len(events))
	points := make([]string, len(events))
	for i, tEvent := range events {
		eventIds[i] = tEvent.EventID
		tripIds[i] = tEvent.TripID
		timestamps[i] = tEvent.Timestamp
		points[i] = fmt.Sprintf("POINT( %s %s )", tEvent.Longitude, tEvent.Latitude)
	}

	return fmt.Sprintf(`
INSERT INTO escooter_events (
	event_id,
	trip_id,
	timestamp,
	geo_point
)
(SELECT *
	FROM  UNNEST(
	[%s],
	[%s],
	[%s],
	[%s]
	)
);`,
		joinAndQuoteStrings(eventIds),
		joinAndQuoteStrings(tripIds),
		joinAndQuoteStrings(timestamps),
		joinAndQuoteStrings(points),
	)
}

func bulkInsertEventMobilitydbSql(events []TripEvent) string {
	eventIds := make([]string, len(events))
	tripIds := make([]string, len(events))
	timestamps := make([]string, len(events))
	geo_points := make([]string, len(events))
	for i, tEvent := range events {
		eventIds[i] = tEvent.EventID
		tripIds[i] = tEvent.TripID
		timestamps[i] = tEvent.Timestamp
		geo_points[i] = fmt.Sprintf("SRID=4326;POINT(%s %s)", tEvent.Longitude, tEvent.Latitude)
	}

	return fmt.Sprintf(`
INSERT INTO escooter_events (
event_id, 
trip_id,
timestamp,
geo_point
)
(SELECT *
FROM  UNNEST(
ARRAY[%s]::UUID[],
ARRAY[%s]::UUID[],
ARRAY[%s]::TIMESTAMPTZ[],
ARRAY[%s]::geometry(Point, 4326)[]
));`,
		joinAndQuoteStrings(eventIds),
		joinAndQuoteStrings(tripIds),
		joinAndQuoteStrings(timestamps),
		joinAndQuoteStrings(geo_points),
	)
}

func importEventsIntoTrips(ctx context.Context, connString string) error {
	startTime := time.Now()
	logger.Info("Importing escooter_events into trips table", "startTime", startTime)

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("Unable to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	query := `
INSERT INTO trips
SELECT trip_id, tgeogpointseq(array_agg(tgeogpoint(geo_point, timestamp) ORDER BY timestamp)) AS trip
FROM escooter_events
GROUP BY trip_id
ON CONFLICT (trip_id) DO UPDATE
	SET trip = EXCLUDED.trip;`

	_, err = conn.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("Executing insert to trips from escooter events: %w", err)
	}

	endTime := time.Now()
	logger.Info("Finished importing escooter_events into trips table", "startTime", startTime, "endTime", endTime, "durationInS", endTime.Sub(startTime).Seconds())
	return nil
}
