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
	switch dbTarget {
	case MobilityDB:
		err := importEventsIntoTrips(ctx, connString)
		if err != nil {
			logger.Error("Error during import of events into trips table", "error", err)
			os.Exit(1)
		}
	case CrateDB:
		err := importEventsIntoTripSummaries(ctx, connString)
		if err != nil {
			logger.Error("Error during import of events into trip_summaries table", "error", err)
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

func importEventsIntoTripSummaries(ctx context.Context, connString string) error {
	startTime := time.Now()
	logger.Info("Importing escooter_events into trip_summaries table", "startTime", startTime)

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("Unable to connect to database: %w", err)
	}
	defer conn.Close(ctx)

	// First, get total number of distinct trips
	var totalTrips int
	err = conn.QueryRow(ctx, "SELECT COUNT(DISTINCT trip_id) FROM escooter_events").Scan(&totalTrips)
	if err != nil {
		return fmt.Errorf("Getting total trip count: %w", err)
	}

	logger.Info("Processing trips in batches", "totalTrips", totalTrips)

	// Process in batches of 1000 trips to avoid memory issues
	batchSize := 1000
	totalBatches := (totalTrips + batchSize - 1) / batchSize
	
	for batch := 0; batch < totalBatches; batch++ {
		offset := batch * batchSize
		
		logger.Info("Processing batch", "batch", batch+1, "totalBatches", totalBatches, "offset", offset)

		batchQuery := `
INSERT INTO trip_summaries (
	trip_id, start_time, end_time, start_point, end_point, 
	trip_length_m, trip_duration_s, point_count
)
WITH batch_trips AS (
	SELECT DISTINCT trip_id 
	FROM escooter_events 
	ORDER BY trip_id 
	LIMIT $1 OFFSET $2
),
trip_endpoints AS (
	SELECT 
		e.trip_id,
		MIN(e.timestamp) AS start_time,
		MAX(e.timestamp) AS end_time
	FROM escooter_events e
	JOIN batch_trips bt ON e.trip_id = bt.trip_id
	GROUP BY e.trip_id
),
trip_start_points AS (
	SELECT DISTINCT 
		e.trip_id,
		e.geo_point AS start_point
	FROM escooter_events e
	JOIN trip_endpoints te ON e.trip_id = te.trip_id AND e.timestamp = te.start_time
),
trip_end_points AS (
	SELECT DISTINCT 
		e.trip_id,
		e.geo_point AS end_point
	FROM escooter_events e
	JOIN trip_endpoints te ON e.trip_id = te.trip_id AND e.timestamp = te.end_time
),
trip_metrics AS (
	SELECT 
		trip_id,
		SUM(CASE WHEN next_point IS NOT NULL THEN distance(geo_point, next_point) ELSE 0 END) AS trip_length_m,
		COUNT(*) AS point_count
	FROM (
		SELECT 
			e.trip_id, e.geo_point,
			LEAD(e.geo_point) OVER (PARTITION BY e.trip_id ORDER BY e.timestamp) AS next_point
		FROM escooter_events e
		JOIN batch_trips bt ON e.trip_id = bt.trip_id
	) windowed_events
	GROUP BY trip_id
)
SELECT 
	te.trip_id,
	te.start_time,
	te.end_time,
	tsp.start_point,
	tep.end_point,
	tm.trip_length_m,
	EXTRACT(EPOCH FROM (te.end_time - te.start_time)) AS trip_duration_s,
	tm.point_count
FROM trip_endpoints te
JOIN trip_start_points tsp ON te.trip_id = tsp.trip_id
JOIN trip_end_points tep ON te.trip_id = tep.trip_id
JOIN trip_metrics tm ON te.trip_id = tm.trip_id;`

		_, err = conn.Exec(ctx, batchQuery, batchSize, offset)
		if err != nil {
			return fmt.Errorf("Executing batch %d insert to trip_summaries: %w", batch+1, err)
		}

		// Log progress every 10 batches
		if (batch+1)%10 == 0 || batch+1 == totalBatches {
			logger.Info("Batch progress", "completedBatches", batch+1, "totalBatches", totalBatches, "progress", fmt.Sprintf("%.1f%%", float64(batch+1)/float64(totalBatches)*100))
		}
	}

	endTime := time.Now()
	logger.Info("Finished importing escooter_events into trip_summaries table", "startTime", startTime, "endTime", endTime, "durationInS", endTime.Sub(startTime).Seconds(), "totalTrips", totalTrips, "totalBatches", totalBatches)
	return nil
}
