package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path"
	"path/filepath"
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

type Locality struct {
	LocalityID string          `json:"locality_id"`
	Name       string          `json:"name"`
	Geometry   json.RawMessage `json:"geometry"`
}

func (d Locality) String() string {
	return fmt.Sprintf("Locality(LocalityID=%s, Name=%s, len(Geometry)=%d)", d.LocalityID, d.Name, len(d.Geometry))
}

// not parsed to correct data types to increase performance
type TripEvent struct {
	EventID   string // UUID
	TripID    string // UUID
	Timestamp string // ISO timestamp
	Latitude  string
	Longitude string
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
		dbTargetStr     = flag.String("dbTarget", "cratedb", "Target database: cratedb or mobilitydbc")
		connString      = flag.String("db", "postgresql://crate:crate@localhost:5432/doc", "Connection string to use to connect to db")
		localitiesPath  = flag.String("localities", "../dataset-generator/output/berlin-localities.geojson", "Path to a file containing localities")
		poisPath        = flag.String("pois", "../dataset-generator/output/berlin-pois.csv", "Path to a file containing POIs")
		tripsPath       = flag.String("trips", "../dataset-generator/output/escooter-trips-small.csv", "Path to a CSV file containing the escooter trip events")
		migrationsDir   = flag.String("migrations", "./migrations", "Directory containing migration files")
		mode            = flag.String("mode", "insert", "Mode: insert, query, init")
		numWorkers      = flag.Int("nworkers", 24, "Number of simultanious workers for the benchmark to use")
		batchSize       = flag.Int("batch-size", 1000, "Number of trip events to insert per sent request")
		useBulkInsert   = flag.Bool("bulk-insert", false, "Insert rows using UNNEST, one query with many inserts")
		logLevel        = flag.String("log", "INFO", "Set <level> for logging. Available: DEBUG, INFO, WARN")
		numQueries      = flag.Int("nqueries", 100, "Number of queries to execute")
		randomSeed      = flag.Int64("seed", 42, "Random seed for deterministic query generation")
		queriesFilepath = flag.String("queries", "./schemas/cratedb-simple-read-queries.tmpl", "Path to a file containing query templates")
	)
	flag.Parse()

	level := slog.LevelInfo
	switch *logLevel {
	case "DEBUG":
		level = slog.LevelDebug
	case "INFO":
		level = slog.LevelInfo
	case "WARN":
		level = slog.LevelWarn
	default:
		fmt.Printf("Unknown logging level: %s", *logLevel)
		os.Exit(1)
	}

	os.MkdirAll("./logs", 0777)

	// Create log filename with timestamp and CLI arguments
	timestamp := time.Now().Format("20060102_150405")
	logFilename := fmt.Sprintf("load-generator_%s_%s_%s_%dw.log",
		*mode, *dbTargetStr, timestamp, *numWorkers)
	logFilePath := path.Join("logs", logFilename)

	// Create log file
	logFile, err := os.Create(logFilePath)
	if err != nil {
		fmt.Printf("Failed to create log file: %v\n", err)
		os.Exit(1)
	}

	// Create multi-writer for both stdout and file
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	handler := slog.NewJSONHandler(multiWriter, &slog.HandlerOptions{
		Level: level,
	})
	logger = slog.New(handler)

	logger.Info("Log file created", "logFile", logFilePath)

	var dbTarget DBTarget
	switch *dbTargetStr {
	case "cratedb":
		dbTarget = CrateDB
	case "mobilitydbc":
		dbTarget = MobilityDB
	default:
		logger.Error("Invalid CLI argument", "argument", "dbTarget", "value", *dbTargetStr, "expected", "cratedb|mobilitydb")
		os.Exit(1)
	}

	localities := mustLoadLocalities(*localitiesPath)
	logger.Info("Loaded and parsed localities", "count", len(localities))

	pois := mustLoadPOIs(*poisPath)
	logger.Info("Loaded and parsed pois", "count", len(pois))

	switch *mode {
	case "init":
		// initialize tables and insert POIs and Localities
		logger.Info("Starting load-generator with following cli arguments",
			"mode", *mode,
			"log", *logLevel,
			"connString", *connString,
			"dbTarget", dbTarget.String(),
			"pois", *poisPath,
			"localities", *localitiesPath,
			"migrations", *migrationsDir,
		)
		mustInitializeDb(ctx, *connString, dbTarget, pois, localities, *migrationsDir)

	case "insert":
		logger.Info("Starting load-generator with following cli arguments",
			"mode", *mode,
			"log", *logLevel,
			"db", dbTarget.String(),
			"nworkers", *numWorkers,
			"batchSize", *batchSize,
			"useBulkInsert", *useBulkInsert,
			"trips", *tripsPath,
		)
		csvFile := createInsertCSVFile(dbTarget, *numWorkers, *batchSize, *useBulkInsert, *tripsPath)
		defer csvFile.Close()
		csvWriter := csv.NewWriter(csvFile)
		defer csvWriter.Flush()

		benchmarkInserts(ctx, *connString, *numWorkers, *batchSize, *useBulkInsert, dbTarget, *tripsPath, csvWriter)

	case "query":
		logger.Info("Starting load-generator with following cli arguments",
			"mode", *mode,
			"log", *logLevel,
			"connString", *connString,
			"nworkers", *numWorkers,
			"dbTarget", dbTarget.String(),
			"trips", *tripsPath,
			"localities", *localitiesPath,
			"pois", *poisPath,
			"qtemplates", *queriesFilepath,
			"numQueries", *numQueries,
			"seed", *randomSeed,
		)
		queryTemplates := mustLoadTemplates(*queriesFilepath)
		logger.Info("Loaded read queries templates", "count", len(queryTemplates.Templates()))

		csvFile := createQueryCSVFile(dbTarget, *numWorkers, *numQueries, *queriesFilepath)
		defer csvFile.Close()
		csvWriter := csv.NewWriter(csvFile)
		defer csvWriter.Flush()

		benchmarkQueries(ctx, *connString, *numWorkers, dbTarget, *tripsPath, localities, pois, queryTemplates, *numQueries, *randomSeed, csvWriter)

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

func mustLoadLocalities(path string) []Locality {
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
	var localities []Locality
	for _, feat := range fc.Features {
		d := Locality{
			LocalityID: feat.Properties["locality_id"].(string),
			Name:       feat.Properties["name"].(string),
			Geometry:   feat.Geometry,
		}
		localities = append(localities, d)
	}
	return localities
}

func mustLoadTemplates(templatesFilepath string) *template.Template {
	allTemplates := template.Must(template.ParseFiles(templatesFilepath))

	// filter out the tempate with the file name
	queryTemplates := template.New("").Option("missingkey=error")
	for _, tmpl := range allTemplates.Templates() {
		if tmpl.Name() == filepath.Base(templatesFilepath) {
			continue
		}
		// Re-parse the content of each template into the new set
		_, err := queryTemplates.New(tmpl.Name()).Parse(tmpl.Root.String())
		if err != nil {
			logger.Error("Error parising a template")
		}
	}
	return queryTemplates
}

func createInsertCSVFile(dbTarget DBTarget, numWorkers, batchSize int, useBulkInsert bool, tripsPath string) *os.File {
	timestamp := time.Now().Format("20060102_150405")
	tripsBasename := strings.TrimSuffix(filepath.Base(tripsPath), filepath.Ext(tripsPath))

	var bulkStr string
	if useBulkInsert {
		bulkStr = "bulk"
	} else {
		bulkStr = "batch"
	}

	filename := fmt.Sprintf("results_insert_%s_%s_%dw_%db_%s_%s.csv",
		dbTarget.String(), tripsBasename, numWorkers, batchSize, bulkStr, timestamp)
	filename = path.Join("results", filename)

	os.MkdirAll("./results", 0777)

	file, err := os.Create(filename)
	if err != nil {
		logger.Error("Failed to create insert CSV file", "filename", filename, "error", err)
		os.Exit(1)
	}

	logger.Info("Created insert results CSV file", "filename", filename)
	return file
}

func createQueryCSVFile(dbTarget DBTarget, numWorkers, numQueries int, queriesPath string) *os.File {
	timestamp := time.Now().Format("20060102_150405")
	queriesBasename := strings.TrimSuffix(filepath.Base(queriesPath), filepath.Ext(queriesPath))

	filename := fmt.Sprintf("results_query_%s_%s_%dw_%dq_%s.csv",
		dbTarget.String(), queriesBasename, numWorkers, numQueries, timestamp)
	filename = path.Join("results", filename)

	os.MkdirAll("./results", 0777)

	file, err := os.Create(filename)
	if err != nil {
		logger.Error("Failed to create query CSV file", "filename", filename, "error", err)
		os.Exit(1)
	}

	logger.Info("Created query results CSV file", "filename", filename)
	return file
}
