package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/csv"
	"io"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/jackc/pgx/v5"
)

func benchmarkQueries(ctx context.Context, connString string, numWorkers int, dbTarget DBTarget, tripEventsCSV string, districts []District, pois []POI, queryTemplates *template.Template, numQueries int, seed int64) {
	logger.Info("Starting Query Benchmark",
		"dbConnString", connString,
		"numWorkers", numWorkers,
		"dbTarget", dbTarget.String(),
		"queriesNum", numQueries,
		"seed", seed,
	)

	tripIds := ReadTripIds(ctx, tripEventsCSV)

	// Create field generator
	generator := NewQueryFieldGenerator(seed, districts, pois, tripIds)

	queryTemplates = queryTemplates.Option("missingkey=error")
	err := ValidateTemplates(ctx, queryTemplates, connString, generator)
	if err != nil {
		logger.Error("Not all templates passed the validation, stopping benchmark", "error", err)
		return
	}
	logger.Info("Using query templates", "count", len(queryTemplates.Templates()))

	// Start workers
	readyStatus := make(chan int, numWorkers)
	jobs := make(chan QueryJob, runtime.NumCPU()*100) // larger buffer to combat workers waiting for main thread to read the csv file
	var wg sync.WaitGroup
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			queryWorker(ctx, id, connString, queryTemplates, jobs, readyStatus)
			wg.Done()
		}(i)
	}
	logger.Info("Started query worker threads", "numWorkers", numWorkers)

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

	templateNames := make([]string, len(queryTemplates.Templates()))
	for i, tmpl := range queryTemplates.Templates() {
		templateNames[i] = tmpl.Name()
	}

	// Wait for all workers to complete
	startTime := time.Now()
	for i := range numQueries {
		if ctx.Err() != nil {
			break
		}
		fields := generator.GenerateFields(i)
		randTmplName := templateNames[i%len(templateNames)]
		jobs <- QueryJob{
			Fields:       fields,
			TemplateName: randTmplName,
		}
	}
	close(jobs)
	wg.Wait()
	endTime := time.Now()
	if ctx.Err() == nil {
		logger.Info("All query workers finished",
			"totalQueries", numQueries,
			"timeElapsedInSec", endTime.Sub(startTime).Seconds(),
			"startTime", startTime,
			"endTime", endTime,
		)
	}
}

func ValidateTemplates(ctx context.Context, templates *template.Template, connString string, generator *QueryFieldGenerator) error {
	templates = templates.Option("missingkey=error")

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	templateNames := make([]string, len(templates.Templates()))
	for i, tmpl := range templates.Templates() {
		templateNames[i] = tmpl.Name()
	}

	logger.Info("Validating the templates by running all the query types on database", "templateNames", templateNames)

	fields := generator.GenerateFields(0)

	for _, tmpl := range templates.Templates() {
		// Execute template with generated fields
		var query strings.Builder
		if err := templates.ExecuteTemplate(&query, tmpl.Name(), fields); err != nil {
			logger.Error("Template validation failed on template execution - contains undefined fields", "template", tmpl.Name(), "error", err, "fields", fields)
			return err
		}

		rows, err := conn.Query(ctx, query.String())
		if err != nil {
			logger.Error("Template validation failed on querying the database", "template", tmpl.Name(), "error", err, "query", query.String())
			rows.Close()
			return err
		}
		rows.Close()

		logger.Debug("Template validation passed", "template", tmpl.Name())
	}
	return nil
}

func ReadTripIds(ctx context.Context, tripEventsCSV string) []string {
	// open the csv file
	f, err := os.Open(tripEventsCSV)
	if err != nil {
		logger.Error("Error opening file", "error", err, "filename", tripEventsCSV)
		os.Exit(1)
	}
	defer f.Close()
	r := csv.NewReader(f)

	// read header of csv
	if _, err := r.Read(); err != nil {
		logger.Error("Error in read pois header", "error", err)
		os.Exit(1)
	}

	tripEventIds := make([]string, 0)
	lastTripId := "" // used to pass only unique values
	for ctx.Err() == nil {
		rec, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			logger.Error("Error in read of trips csv", "error", err)
			os.Exit(1)
		}

		tripId := rec[1]

		if tripId != lastTripId {
			tripEventIds = append(tripEventIds, rec[1])
			lastTripId = tripId
		}
	}
	logger.Debug("Read trip events ids from CSV file", "file", tripEventsCSV, "tripEventsCount", len(tripEventIds))
	return tripEventIds
}

type QueryJob struct {
	TemplateName string
	Fields       QueryFields
}

// queryWorker executes queries
func queryWorker(ctx context.Context, id int, connString string, templates *template.Template, jobs <-chan QueryJob, readyStatus chan<- int) {
	logger.Info("Query worker started", "id", id)

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		logger.Error("Query worker was unable to connect to database, worker stopping", "id", id, "error", err)
		return
	}
	defer conn.Close(ctx)
	logger.Info("Query worker connected to db", "id", id)

	queryIndex := -1
	successfulQueries := 0
	failedQueries := 0

	readyStatus <- id

	defer func() {
		logger.Info(
			"Query worker finished",
			"id", id,
			"executedQueries", queryIndex+1,
			"successfulQueries", successfulQueries,
			"failedQueries", failedQueries,
			"ctxErr", ctx.Err(),
			"usedTemplates", len(templates.Templates()),
		)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-jobs:
			if !ok {
				logger.Info("Worker closing", "id", id)
				return
			}
			queryIndex++

			// Execute template with generated fields
			var query strings.Builder
			if err := templates.ExecuteTemplate(&query, job.TemplateName, job.Fields); err != nil {
				logger.Error("Query worker failed to execute template", "id", id, "template", job.TemplateName, "error", err, "fields", job.Fields)
				continue
			}

			logger.Debug("Query worker executing query", "id", id, "query", query.String(), "template", job.TemplateName, "fields", job.Fields)
			querySuccessful := true
			resultingRowsCount := 0
			startTime := time.Now()
			rows, err := conn.Query(ctx, query.String())
			if err != nil {
				querySuccessful = false
				logger.Debug("Query worker query failed", "id", id, "error", err)
			} else {
				// consume the resulting rows
				rowNum := -1
				for rows.Next() {
					rowNum++
					rowVals, err := rows.Values()
					if err != nil {
						// This shouldn't happen as we first check with rows.Next if a value exist
						querySuccessful = false
						logger.Debug("Query worker query failed when reading values of a resulting rows", "id", id, "rowNum", rowNum, "error", err)
					}

					logger.Debug("Query worker query resulted in row", "id", id, "rowNum", rowNum, "error", err, "values", rowVals)
					resultingRowsCount++
				}
				if err = rows.Err(); err != nil {
					querySuccessful = false
					logger.Debug("Query worker query failed when reading resulting rows", "id", id, "error", err)
				}
				rows.Close()
			}

			if querySuccessful {
				successfulQueries++
			} else {
				failedQueries++
			}

			endTime := time.Now()
			queryDuration := endTime.Sub(startTime)

			logger.Info("Query worker finished query",
				"workerId", id,
				"jobType", "query",
				"templateName", job.TemplateName,
				"queryDurationInMs", queryDuration.Milliseconds(),
				"startTime", startTime,
				"endTime", endTime,
				"successful", querySuccessful,
				"resultingRowsCount", resultingRowsCount,
				"queryIndex", queryIndex,
				"error", err,
			)
		}
	}
}

// QueryFieldGenerator generates random query parameters in a seeded, deterministic manner
type QueryFieldGenerator struct {
	baseSeed int64

	// Real data pools from loaded files
	districts []District
	pois      []POI
	tripIDs   []string

	// Time bounds for realistic queries
	minTime time.Time
	maxTime time.Time
}

// QueryFields contains all possible template parameters
type QueryFields struct {
	DistrictName string
	EndTime      string // RFC3339 string
	Limit        int
	POIID        string
	Radius       float64
	StartTime    string // RFC3339 string
	Timestamp    string // RFC3339 string
	TripID       string
}

// NewQueryFieldGenerator creates a new seeded field generator
func NewQueryFieldGenerator(seed int64, districts []District, pois []POI, tripIds []string) *QueryFieldGenerator {
	// Load Berlin time zone
	berlinLoc, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		panic("Failed to load Europe/Berlin timezone: " + err.Error())
	}

	// Set realistic time bounds (adjust based on your dataset)
	minTime := time.Date(2020, 1, 1, 0, 0, 0, 0, berlinLoc)
	maxTime := time.Date(2025, 12, 31, 23, 59, 59, 0, berlinLoc)

	return &QueryFieldGenerator{
		baseSeed:  seed,
		districts: districts,
		pois:      pois,
		tripIDs:   tripIds,
		minTime:   minTime,
		maxTime:   maxTime,
	}
}

// GenerateFields generates all query fields for a specific worker and query index
func (g *QueryFieldGenerator) GenerateFields(queryIndex int) QueryFields {
	// Create single deterministic seed for this specific query
	hash := sha256.New()
	binary.Write(hash, binary.LittleEndian, g.baseSeed)
	binary.Write(hash, binary.LittleEndian, queryIndex)

	hashBytes := hash.Sum(nil)
	seed := int64(binary.LittleEndian.Uint64(hashBytes[:8]))

	// Create single RNG for all fields in this query
	rng := rand.New(rand.NewSource(seed))

	// Generate start time first
	timeRange := g.maxTime.Unix() - g.minTime.Unix()
	startOffset := rng.Int63n(timeRange - 3600) // Leave 1 hour for EndTime
	startTime := time.Unix(g.minTime.Unix()+startOffset, 0)

	// Generate end time after start time (1 minute to 1 hour later)
	minDuration := int64(60)   // 1 minute
	maxDuration := int64(3600) // 1 hour
	duration := minDuration + rng.Int63n(maxDuration-minDuration)
	endTime := startTime.Add(time.Duration(duration) * time.Second)

	// Generate single timestamp within reasonable bounds
	timestampOffset := rng.Int63n(timeRange)
	timestamp := time.Unix(g.minTime.Unix()+timestampOffset, 0)

	return QueryFields{
		DistrictName: g.districts[rng.Intn(len(g.districts))].Name,
		Limit:        5 + rng.Intn(100), // 5-100
		POIID:        g.pois[rng.Intn(len(g.pois))].POIID,
		Radius:       1000 + rng.Float64()*4000, // 1000-5000 meters
		StartTime:    startTime.Format(time.RFC3339),
		EndTime:      endTime.Format(time.RFC3339),
		Timestamp:    timestamp.Format(time.RFC3339),
		TripID:       g.tripIDs[rng.Intn(len(g.tripIDs))],
	}
}
