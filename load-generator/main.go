package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"text/template"
	"time"
)

var logger *slog.Logger

type POI struct {
	POIID     string //UUID
	Name      string
	Category  string
	Longitude string // stored as strings in order not to lose precision compared to CSV file
	Latitude  string
}

type District struct {
	DistrictID string          `json:"district_id"`
	Name       string          `json:"name"`
	Geometry   json.RawMessage `json:"geometry"`
}

func (d District) String() string {
	return fmt.Sprintf("District(DistrictID=%s, Name=%s, len(Geometry)=%d)", d.DistrictID, d.Name, len(d.Geometry))
}

// not parsed to correct data types to increase performance
type TripEvent struct {
	EventID   string // UUID
	TripID    string // UUID
	Timestamp string // ISO timestamp
	Latitude  string
	Longitude string
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

type DBTarget int

const (
	CrateDB    DBTarget = 0
	MobilityDB DBTarget = 1
)

func (target DBTarget) String() string {
	switch target {
	case CrateDB:
		return "crateDB"
	case MobilityDB:
		return "mobilityDB"
	}
	logger.Error("Trying to get String value of a non existant target", "target", target)
	os.Exit(1)
	return ""
}

func main() {
	// utilize all cores
	runtime.GOMAXPROCS(runtime.NumCPU())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()
	// CLI flags
	var (
		dbTargetStr        = flag.String("db", "cratedb", "Target database: cratedb or mobilitydb")
		districtsPath      = flag.String("districts", "../dataset-generator/output/berlin-districts.geojson", "Path to a file containing districts")
		poisPath           = flag.String("pois", "../dataset-generator/output/berlin-pois.csv", "Path to a file containing POIs")
		tripsPath          = flag.String("trips", "../dataset-generator/output/escooter-trips-small.csv", "Path to a CSV file containing the escooter trip events")
		ddlPath            = flag.String("ddl", "./schemas/cratedb-ddl.sql", "File containing the DDL for creating database tables")
		mode               = flag.String("mode", "insert", "Mode: insert, simple-query, or complex-query")
		numWorkers         = flag.Int("nworkers", 100, "Number of simultanious workers for the benchmark to use")
		skipInitialization = flag.Bool("skip-init", false, "Skip database initialization (creating tables, inserting POIs, and districts")
		logDebug           = flag.Bool("log-debug", false, "Turn on the DEBUG level for logging")
		queriesPerWorker   = flag.Int("queries-per-worker", 100, "Number of queries each worker should execute")
		randomSeed         = flag.Int64("seed", 42, "Random seed for deterministic query generation")
		templatesFilepath  = flag.String("qtemplates", "./schemas/cratedb-simple-read-queries.tmpl", "Path to a file containing query templates")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *logDebug {
		level = slog.LevelDebug
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})
	logger = slog.New(handler)

	var connString string
	var dbTarget DBTarget
	switch *dbTargetStr {
	case "cratedb":
		dbTarget = CrateDB
		connString = "postgresql://crate:crate@localhost:5432/doc"
	case "mobilitydb":
		dbTarget = MobilityDB
		connString = "postgres://user:pass@localhost:5432/yourdb"
	default:
		logger.Error("Invalid CLI argument", "argument", "dbTarget", "value", *dbTargetStr, "expected", "cratedb|mobilitydb")
		os.Exit(1)
	}

	districts := mustLoadDistricts(*districtsPath)
	logger.Info("Loaded and parsed districts", "count", len(districts))

	pois := mustLoadPOIs(*poisPath)
	logger.Info("Loaded and parsed pois", "count", len(pois))

	queryTemplates := mustLoadAndValidateTemplates(*templatesFilepath)
	logger.Info("Loaded read queries templates", "count", len(queryTemplates.Templates()))

	if *skipInitialization {
		logger.Info("Skipping initialization because of the CLI flag")
	} else {
		ddlB, err := os.ReadFile(*ddlPath)
		if err != nil {
			logger.Error("Error reading DDL file", "error", err)
			os.Exit(1)
		}
		ddl := string(ddlB)
		mustInitializeDb(ctx, connString, dbTarget, pois, districts, ddl)
	}

	switch *mode {
	case "insert":
		benchmarkInserts(ctx, connString, *numWorkers, dbTarget, *tripsPath)
	case "simple-query":
		benchmarkQueries(ctx, connString, *numWorkers, dbTarget, districts, pois, queryTemplates, *queriesPerWorker, *randomSeed)
	case "complex-query":
		// benchmarkQueries(ctx, connString, *numWorkers, dbTarget, districts, pois, *schemasPath, *queriesPerWorker, *randomSeed, "complex")
	default:
		logger.Error("unknown mode", "mode", *mode)
		os.Exit(1)
	}
}

func mustLoadPOIs(path string) []POI {
	f, err := os.Open(path)
	if err != nil {
		logger.Error("Error in open pois.csv", "error", err)
		os.Exit(1)
	}
	defer f.Close()
	r := csv.NewReader(f)
	// read header
	if _, err := r.Read(); err != nil {
		logger.Error("Error in read pois header", "error", err)
		os.Exit(1)
	}
	var pois []POI
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			logger.Error("Error in read pois record", "error", err)
			os.Exit(1)
		}

		var p POI
		p.POIID = rec[0]
		p.Name = strings.ReplaceAll(rec[1], "'", "''")
		p.Category = rec[2]
		p.Longitude = rec[3]
		p.Latitude = rec[4]

		pois = append(pois, p)
	}
	return pois
}

func mustLoadDistricts(path string) []District {
	b, err := os.ReadFile(path)
	if err != nil {
		logger.Error("Error in read geojson", "error", err)
		os.Exit(1)
	}
	// GeoJSON FeatureCollection
	var fc struct {
		Features []struct {
			Properties map[string]any  `json:"properties"`
			Geometry   json.RawMessage `json:"geometry"`
		} `json:"features"`
	}
	if err := json.Unmarshal(b, &fc); err != nil {
		logger.Error("Error in parse geojson", "error", err)
		os.Exit(1)
	}
	var districts []District
	for _, feat := range fc.Features {
		d := District{
			DistrictID: feat.Properties["district_id"].(string),
			Name:       feat.Properties["name"].(string),
			Geometry:   feat.Geometry,
		}
		districts = append(districts, d)
	}
	return districts
}

func mustLoadAndValidateTemplates(templatesFilepath string) *template.Template {
	templates := template.Must(template.New("").ParseFiles(templatesFilepath))
	templates = templates.Option("missingkey=error")

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

	for _, tmpl := range templates.Templates() {
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
	return templates
}
