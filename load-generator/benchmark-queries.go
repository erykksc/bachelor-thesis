package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/jackc/pgx/v5"
)

func benchmarkQueries(ctx context.Context, connString string, numWorkers int, dbTarget DBTarget, districts []District, pois []POI, queryTemplates *template.Template, queriesPerWorker int, seed int64) {
	logger.Info("Starting Query Benchmark",
		"dbConnString", connString,
		"numWorkers", numWorkers,
		"dbTarget", dbTarget,
		"queriesPerWorker", queriesPerWorker,
		"seed", seed,
	)

	// Validate that all templates work with our QueryFields structure
	testFields := QueryFields{
		DistrictName: "TestDistrict",
		EndTime:      time.Now().Add(time.Hour),
		Limit:        10,
		POIID:        "test-poi-id",
		Radius:       100.0,
		StartTime:    time.Now(),
		Timestamp:    time.Now(),
		TripID:       "test-trip-id",
	}

	for _, tmpl := range queryTemplates.Templates() {
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, testFields); err != nil {
			logger.Error("Template validation failed - contains undefined fields",
				"template", tmpl.Name(),
				"error", err,
			)
			os.Exit(1)
		}
		logger.Debug("Template validation passed", "template", tmpl.Name())
	}

	logger.Info("Using query templates", "count", len(queryTemplates.Templates()))

	// Create field generator
	generator := NewQueryFieldGenerator(seed, districts, pois)

	// Start workers
	var wg sync.WaitGroup
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			queryWorker(ctx, id, generator, queryTemplates, &wg, connString, queriesPerWorker)
		}(i)
	}

	logger.Info("Started query worker threads", "numWorkers", numWorkers)

	// Wait for all workers to complete
	startTime := time.Now()
	wg.Wait()

	if ctx.Err() == nil {
		totalQueries := numWorkers * queriesPerWorker
		logger.Info("All query workers completed",
			"totalQueries", totalQueries,
			"timeElapsed", time.Since(startTime),
		)
	}
}

// queryWorker executes random queries using the field generator and templates
func queryWorker(ctx context.Context, id int, generator *QueryFieldGenerator, templates *template.Template, wg *sync.WaitGroup, connString string, maxQueries int) {
	defer wg.Done()

	logger.Info("Query worker started", "id", id)

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		logger.Error("Query worker unable to connect to database", "id", id, "error", err)
		return
	}
	defer conn.Close(ctx)
	logger.Info("Query worker connected to db", "id", id)

	queryIndex := int64(id * 1000000) // Offset queries per worker to avoid overlap
	executedQueries := 0
	lastJobFinishTime := time.Now()

	templateNames := make([]string, len(templates.Templates()))
	for i, tmpl := range templates.Templates() {
		templateNames[i] = tmpl.Name()
	}

	for executedQueries < maxQueries {
		select {
		case <-ctx.Done():
			logger.Info("Query worker finished because context is done", "id", id, "executedQueries", executedQueries)
			return
		default:
			// Generate fields for this query
			fields := generator.GenerateFields(queryIndex)

			// Pick random template
			templateName := templateNames[int(queryIndex)%len(templateNames)]

			// Execute template with generated fields
			var buf bytes.Buffer
			if err := templates.ExecuteTemplate(&buf, templateName, fields); err != nil {
				logger.Error("Query worker failed to execute template", "id", id, "template", templateName, "error", err, "fields", fields)
				queryIndex++
				continue
			}

			// Check if template execution resulted in missing field errors
			queryStr := buf.String()
			if strings.Contains(queryStr, "<no value>") {
				logger.Error("Query worker template contains missing fields",
					"id", id,
					"template", templateName,
					"query", queryStr,
					"fields", fields)
				queryIndex++
				continue
			}

			query := queryStr

			waitedTime := time.Since(lastJobFinishTime)
			querySuccessful := true
			startTime := time.Now()

			rows, err := conn.Query(ctx, query)
			if err != nil {
				querySuccessful = false
				logger.Debug("Query worker query failed", "id", id, "error", err)
			} else {
				// Consume all rows to complete the query
				for rows.Next() {
					// Just iterate through results without processing
				}
				rows.Close()
			}

			endTime := time.Now()
			queryDuration := endTime.Sub(startTime)

			logger.Info("Query worker finished query",
				"workerId", id,
				"jobType", "query",
				"templateName", templateName,
				"queryDuration", queryDuration,
				"waitedTime", waitedTime,
				"successful", querySuccessful,
				"queryIndex", queryIndex,
				"error", err,
			)

			lastJobFinishTime = time.Now()
			queryIndex++
			executedQueries++
		}
	}

	logger.Info("Query worker completed all queries", "id", id, "totalQueries", executedQueries)
}

// QueryFieldGenerator generates random query parameters in a seeded, deterministic manner
type QueryFieldGenerator struct {
	baseSeed   int64
	fieldRands map[string]*rand.Rand

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
	EndTime      time.Time
	Limit        int
	POIID        string
	Radius       float64
	StartTime    time.Time
	Timestamp    time.Time
	TripID       string
}

// ValidateTemplateFields checks if a template contains any undefined field references
func (qf QueryFields) ValidateTemplateFields(templateName string, templateContent string) error {
	// Create a test template with strict error checking
	testTemplate, err := template.New("validation").Option("missingkey=error").Parse(templateContent)
	if err != nil {
		return fmt.Errorf("template parsing error: %w", err)
	}

	// Try to execute the template with our fields
	var buf bytes.Buffer
	if err := testTemplate.Execute(&buf, qf); err != nil {
		return fmt.Errorf("template %s contains undefined fields: %w", templateName, err)
	}

	return nil
}

// NewQueryFieldGenerator creates a new seeded field generator
func NewQueryFieldGenerator(seed int64, districts []District, pois []POI) *QueryFieldGenerator {
	// Generate realistic trip IDs pool (you could also load these from actual data)
	tripIDs := make([]string, 10000)
	tripRand := rand.New(rand.NewSource(seed))
	for i := range tripIDs {
		tripIDs[i] = fmt.Sprintf("trip_%06d", tripRand.Intn(100000))
	}

	// Set realistic time bounds (adjust based on your dataset)
	minTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	maxTime := time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)

	generator := &QueryFieldGenerator{
		baseSeed:   seed,
		fieldRands: make(map[string]*rand.Rand),
		districts:  districts,
		pois:       pois,
		tripIDs:    tripIDs,
		minTime:    minTime,
		maxTime:    maxTime,
	}

	// Initialize per-field random generators
	fieldNames := []string{"DistrictName", "EndTime", "Limit", "POIID", "Radius", "StartTime", "Timestamp", "TripID"}
	for _, fieldName := range fieldNames {
		fieldSeed := generator.deriveFieldSeed(fieldName, 0)
		generator.fieldRands[fieldName] = rand.New(rand.NewSource(fieldSeed))
	}

	return generator
}

// deriveFieldSeed creates a deterministic seed for a specific field and query index
func (g *QueryFieldGenerator) deriveFieldSeed(fieldName string, queryIndex int64) int64 {
	hash := sha256.New()
	binary.Write(hash, binary.LittleEndian, g.baseSeed)
	hash.Write([]byte(fieldName))
	binary.Write(hash, binary.LittleEndian, queryIndex)

	hashBytes := hash.Sum(nil)
	return int64(binary.LittleEndian.Uint64(hashBytes[:8]))
}

// GenerateFields generates all query fields for a specific query index
func (g *QueryFieldGenerator) GenerateFields(queryIndex int64) QueryFields {
	// Update field generators with query-specific seeds
	for fieldName, fieldRand := range g.fieldRands {
		fieldSeed := g.deriveFieldSeed(fieldName, queryIndex)
		fieldRand.Seed(fieldSeed)
	}

	// Generate start time first
	timeRange := g.maxTime.Unix() - g.minTime.Unix()
	startOffset := g.fieldRands["StartTime"].Int63n(timeRange - 3600) // Leave 1 hour for EndTime
	startTime := time.Unix(g.minTime.Unix()+startOffset, 0)

	// Generate end time after start time (1 minute to 1 hour later)
	minDuration := int64(60)   // 1 minute
	maxDuration := int64(3600) // 1 hour
	duration := minDuration + g.fieldRands["EndTime"].Int63n(maxDuration-minDuration)
	endTime := startTime.Add(time.Duration(duration) * time.Second)

	// Generate single timestamp within reasonable bounds
	timestampOffset := g.fieldRands["Timestamp"].Int63n(timeRange)
	timestamp := time.Unix(g.minTime.Unix()+timestampOffset, 0)

	return QueryFields{
		DistrictName: g.districts[g.fieldRands["DistrictName"].Intn(len(g.districts))].Name,
		EndTime:      endTime,
		Limit:        1 + g.fieldRands["Limit"].Intn(100), // 1-100
		POIID:        g.pois[g.fieldRands["POIID"].Intn(len(g.pois))].POIID,
		Radius:       50.0 + g.fieldRands["Radius"].Float64()*1950.0, // 50-2000 meters
		StartTime:    startTime,
		Timestamp:    timestamp,
		TripID:       g.tripIDs[g.fieldRands["TripID"].Intn(len(g.tripIDs))],
	}
}
