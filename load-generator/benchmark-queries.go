package main

import (
	"bytes"
	"context"
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

	logger.Info("Using query templates", "count", len(queryTemplates.Templates()), "templates", queryTemplates)

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
