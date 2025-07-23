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
	"runtime"
	"strings"
	"text/template"
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
		mode               = flag.String("mode", "insert", "Mode: insert, query")
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

	switch *mode {
	case "insert":
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
		benchmarkInserts(ctx, connString, *numWorkers, dbTarget, *tripsPath)

	case "query":
		queryTemplates := template.Must(template.New("").ParseFiles(*templatesFilepath))
		logger.Info("Loaded read queries templates", "count", len(queryTemplates.Templates()))
		benchmarkQueries(ctx, connString, *numWorkers, dbTarget, districts, pois, queryTemplates, *queriesPerWorker, *randomSeed)

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
