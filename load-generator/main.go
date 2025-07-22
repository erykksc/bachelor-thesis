package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"text/template"
	"time"

	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"gopkg.in/yaml.v3"
)

var logger *slog.Logger

type ReadQueryDef struct {
	Name        string       `yaml:"name"`
	Description string       `yaml:"description"`
	Parameters  []QueryParam `yaml:"parameters"`
	CrateSQL    string       `yaml:"cratedb_sql"`
	MobSQL      string       `yaml:"mobilitydb_sql"`
}

type QueryParam struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	// CLI flags
	var (
		dbTargetStr        = flag.String("db", "cratedb", "Target database: cratedb or mobilitydb")
		districtsPath      = flag.String("districts", "../dataset-generator/output/berlin-districts.geojson", "Path to a file containing districts")
		poisPath           = flag.String("pois", "../dataset-generator/output/berlin-pois.csv", "Path to a file containing POIs")
		tripsPath          = flag.String("trips", "../dataset-generator/output/escooter-trips-small.csv", "Path to a CSV file containing the escooter trip events")
		ddlPath            = flag.String("ddl", "./schemas/cratedb-ddl.sql", "File containing the DDL for creating database tables")
		mode               = flag.String("mode", "insert", "Mode: insert or query")
		numWorkers         = flag.Int("nworkers", 100, "Number of simultanious workers for the benchmark to use")
		skipInitialization = flag.Bool("skip-init", false, "Skip database initialization (creating tables, inserting POIs, and districts")
		logDebug           = flag.Bool("log-debug", false, "Turn on the DEBUG level for logging")
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

	// readQueries := mustLoadReadQueries(filepath.Join(*schemasPath, "read-queries.yaml"))
	// logger.Info("Loaded read queries templates", "count", len(readQueries))

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
	// benchmarkSimpleQueries()
	case "complex-query":
	//benchmarkComplexQueries()
	default:
		logger.Error("unknown mode", "mode", *mode)
		os.Exit(1)
	}
}

func mustInitializeDb(ctx context.Context, connString string, dbTarget DBTarget, pois []POI, districts []District, ddl string) {
	logger.Info("Initializing Database", "databaseType", dbTarget, "connString", connString, "poiCount", len(pois), "districtCount", len(districts))
	mustInsertPoiToDb := mustInsertPoiToCratedb
	mustInsertDistrictToDb := mustInsertDistrictToCratedb

	switch dbTarget {
	case CrateDB:
		logger.Info("Initializing CrateDB")
		mustInsertPoiToDb = mustInsertPoiToCratedb
		mustInsertDistrictToDb = mustInsertDistrictToCratedb

	case MobilityDB:
		logger.Info("Initializing MobilityDB")
		mustInsertPoiToDb = mustInsertPoiToMobilitydb
		mustInsertDistrictToDb = mustInsertDistrictToMobilitydb
	}

	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		logger.Error("Unable to connect to database", "error", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	// Initialize tables
	res, err := conn.Exec(ctx, ddl)
	if err != nil {
		logger.Error("Error while executing DDL", "database", dbTarget, "result", res, "error", err)
		os.Exit(1)
	}
	logger.Info("Inserted Tables successfully", "res", res)

	// Insert POIs
	startTime := time.Now()
	for _, poi := range pois {
		mustInsertPoiToDb(ctx, conn, &poi)
	}
	logger.Info("Inserted all POIs into database", "dbTarget", dbTarget, "poiCount", len(pois), "timeElapsed", time.Since(startTime))

	// Insert districts
	startTime = time.Now()
	for _, district := range districts {
		mustInsertDistrictToDb(ctx, conn, &district)
	}
	logger.Info("Inserted all districts into database", "dbTarget", dbTarget, "districtCount", len(pois), "timeElapsed", time.Since(startTime))
}

func mustInsertPoiToCratedb(ctx context.Context, conn *pgx.Conn, poi *POI) {
	// string interpolation is done instead of passing arguments to conn.Exec
	// as I want to avoid istalling additional library to support GEO_POINT types
	query := fmt.Sprintf(`INSERT INTO pois(
				poi_id,
				name,
				category,
				geo_point
			) VALUES (
				'%s', '%s', '%s', [%s, %s]
			);`, poi.POIID, poi.Name, poi.Category, poi.Latitude, poi.Longitude)
	cmdTag, err := conn.Exec(ctx, query)
	if err != nil {
		logger.Error("Error executing poi insert query", "error", err, "commandTag", cmdTag.String(), "poiData", poi)
		os.Exit(1)
	}
	logger.Debug("Inserted POI", "poi", poi)
}

func mustInsertPoiToMobilitydb(ctx context.Context, conn *pgx.Conn, poi *POI) {
	query := fmt.Sprintf(`INSERT INTO pois (
		poi_id,
		name,
		category,
		geom
	)
	VALUES (
		%s,
		'%s',
		'%s',
		ST_SetSRID(ST_MakePoint(%s, %s), 4326)
	);`, poi.POIID, poi.Name, poi.Category, poi.Longitude, poi.Latitude)
	cmdTag, err := conn.Exec(ctx, query)
	if err != nil {
		logger.Error("Error executing poi insert query", "error", err, "commandTag", cmdTag.String(), "poiData", poi)
		os.Exit(1)
	}
	logger.Debug("Inserted POI", "poi", poi)
}

func mustInsertDistrictToCratedb(ctx context.Context, conn *pgx.Conn, district *District) {
	query := `INSERT INTO districts( district_id, name, geo_shape)
				VALUES ( $1, $2, $3);`

	cmdTag, err := conn.Exec(ctx, query, district.DistrictID, district.Name, district.Geometry)
	if err != nil {
		logger.Error("Error executing district insert query", "error", err, "commandTag", cmdTag.String(), "districtData", district)
		os.Exit(1)
	}
	logger.Debug("Inserted District", "district", district)
}

func mustInsertDistrictToMobilitydb(ctx context.Context, conn *pgx.Conn, district *District) {
	query := `
		INSERT INTO districts (
			district_id,
			name,
			geom
		)
		VALUES (
			$1,
			$2,
			ST_GeomFromGeoJSON($3)
		);`

	cmdTag, err := conn.Exec(ctx, query, district.DistrictID, district.Name, district.Geometry)
	if err != nil {
		logger.Error("Error executing district insert query", "error", err, "commandTag", cmdTag.String(), "districtData", district)
		os.Exit(1)
	}
	logger.Debug("Inserted District", "district", district)
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

// --- Loading functions ---

func mustLoadReadQueries(queriesPath string) []ReadQueryDef {
	var queries []ReadQueryDef
	// Load queries
	dataQ, err := os.ReadFile(queriesPath)
	if err != nil {
		logger.Error("Error during os.ReadFile", "file", queriesPath, "error", err)
		os.Exit(1)
	}
	if err := yaml.Unmarshal(dataQ, &struct{ Queries *[]ReadQueryDef }{&queries}); err != nil {
		logger.Error("Error during yaml.Unmarshal", "dataFromFile", queriesPath, "error", err)
		os.Exit(1)
	}
	return queries
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

// --- Benchmark modes/strategies ---

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

func benchmarkSimpleQueries(db *sql.DB, target string, queries []ReadQueryDef, districts []District, pois []POI) {
	// prepare templates
	tmpls := make(map[string]*template.Template)
	for _, def := range queries {
		var sqlT string
		if target == "cratedb" {
			sqlT = def.CrateSQL
		} else {
			sqlT = def.MobSQL
		}
		tmpls[def.Name] = template.Must(template.New(def.Name).Parse(sqlT))
	}
	// example: pick a random query, fill params from districts/pois, execute...
	// TODO: implement random selection, param generation, timing, and db.Query(...)
}

func benchmarkComplexQueries(db *sql.DB, target string, queries []ReadQueryDef, districts []District, pois []POI) {
	// prepare templates
	tmpls := make(map[string]*template.Template)
	for _, def := range queries {
		var sqlT string
		if target == "cratedb" {
			sqlT = def.CrateSQL
		} else {
			sqlT = def.MobSQL
		}
		tmpls[def.Name] = template.Must(template.New(def.Name).Parse(sqlT))
	}
	// example: pick a random query, fill params from districts/pois, execute...
	// TODO: implement random selection, param generation, timing, and db.Query(...)
}
