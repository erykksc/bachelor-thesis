// cmd/benchmark/main.go
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"text/template"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"    // Postgres/MobilityDB
	_ "github.com/lucasjones/pgx-cratedb" // CrateDB HTTP driver (example)
	"gopkg.in/yaml.v3"
)

// --- Structs for YAML parsing ---

type QueryDef struct {
	Name        string  `yaml:"name"`
	Description string  `yaml:"description"`
	Parameters  []Param `yaml:"parameters"`
	CrateSQL    string  `yaml:"cratedb_sql"`
	MobSQL      string  `yaml:"mobilitydb_sql"`
}

type InsertDef struct {
	Name        string  `yaml:"name"`
	Description string  `yaml:"description"`
	Parameters  []Param `yaml:"parameters"`
	CrateSQL    string  `yaml:"cratedb_sql"`
	MobSQL      string  `yaml:"mobilitydb_sql"`
}

type Param struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

type TemplateCatalog struct {
	Queries    []QueryDef  `yaml:"queries"`
	Insertions []InsertDef `yaml:"insert_queries"`
}

// --- POI & District structs ---

type POI struct {
	POIID     string
	Name      string
	Category  string
	Longitude float64
	Latitude  float64
}

type District struct {
	DistrictID string          `json:"district_id"`
	Name       string          `json:"name"`
	Geometry   json.RawMessage `json:"geometry"`
}

func main() {
	// --- CLI flags ---
	var (
		dbTarget = flag.String("db", "cratedb", "Target database: cratedb or mobilitydb")
		mode     = flag.String("mode", "insert", "Mode: insert or query")
	)
	flag.Parse()

	// --- Load small data into memory ---
	districts := loadDistricts("output/berlin_districts.geojson")
	pois := loadPOIs("output/berlin_pois.csv")

	// --- Load YAML templates ---
	catalog := loadYAML("schemas/queries.yaml", "schemas/insert_queries.yaml")

	// --- Load DDL files ---
	cratedbDDL := mustReadFile("schemas/cratedb-ddl.sql")
	mobilityDDL := mustReadFile("schemas/mobilitydb-ddl.sql")

	// --- Connect to chosen DB ---
	db := openDB(*dbTarget)
	defer db.Close()

	// Optionally: execute DDL once
	if err := execDDL(db, *dbTarget, cratedbDDL, mobilityDDL); err != nil {
		log.Fatalf("failed DDL: %v", err)
	}

	switch *mode {
	case "insert":
		benchmarkInsert(db, *dbTarget, catalog.Insertions)
	case "query":
		benchmarkQuery(db, *dbTarget, catalog.Queries, districts, pois)
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
}

// --- Loading functions ---

func loadYAML(queriesPath, insertsPath string) *TemplateCatalog {
	out := &TemplateCatalog{}
	// Load queries
	dataQ, err := io.ReadFile(queriesPath)
	if err != nil {
		log.Fatalf("read queries.yaml: %v", err)
	}
	if err := yaml.Unmarshal(dataQ, &struct{ Queries *[]QueryDef }{&out.Queries}); err != nil {
		log.Fatalf("parse queries.yaml: %v", err)
	}
	// Load insert_queries
	dataI, err := ioutil.ReadFile(insertsPath)
	if err != nil {
		log.Fatalf("read insert_queries.yaml: %v", err)
	}
	if err := yaml.Unmarshal(dataI, &struct{ InsertQueries *[]InsertDef }{&out.Insertions}); err != nil {
		log.Fatalf("parse insert_queries.yaml: %v", err)
	}
	return out
}

func loadPOIs(path string) []POI {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("open pois.csv: %v", err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	// read header
	if _, err := r.Read(); err != nil {
		log.Fatalf("read pois header: %v", err)
	}
	var pois []POI
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("read pois record: %v", err)
		}
		var p POI
		p.POIID = rec[0]
		p.Name = rec[1]
		p.Category = rec[2]
		fmt.Sscanf(rec[3], "%f", &p.Longitude)
		fmt.Sscanf(rec[4], "%f", &p.Latitude)
		pois = append(pois, p)
	}
	return pois
}

func loadDistricts(path string) []District {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("read geojson: %v", err)
	}
	// GeoJSON FeatureCollection
	var fc struct {
		Features []struct {
			Properties map[string]interface{} `json:"properties"`
			Geometry   json.RawMessage        `json:"geometry"`
		} `json:"features"`
	}
	if err := json.Unmarshal(b, &fc); err != nil {
		log.Fatalf("parse geojson: %v", err)
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

func mustReadFile(path string) string {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// --- DB setup ---

func openDB(target string) *sql.DB {
	var dsn string
	switch target {
	case "cratedb":
		dsn = "http://crate:4200?username=crate&password=crate"
		return must(sql.Open("pgx-cratedb", dsn))
	case "mobilitydb":
		dsn = "postgres://user:pass@localhost:5432/yourdb"
		return must(sql.Open("pgx", dsn))
	default:
		log.Fatalf("unknown db target %q", target)
		return nil
	}
}

func execDDL(db *sql.DB, target, crateDDL, mobDDL string) error {
	var ddl string
	if target == "cratedb" {
		ddl = crateDDL
	} else {
		ddl = mobDDL
	}
	_, err := db.Exec(ddl)
	return err
}

func must(db *sql.DB, err error) *sql.DB {
	if err != nil {
		log.Fatalf("db open: %v", err)
	}
	return db
}

// --- Benchmark modes ---

func benchmarkInsert(db *sql.DB, target string, inserts []InsertDef) {
	start := time.Now()
	// prepare all templates
	tmpls := make(map[string]*template.Template)
	for _, def := range inserts {
		var sqlT string
		if target == "cratedb" {
			sqlT = def.CrateSQL
		} else {
			sqlT = def.MobSQL
		}
		tmpls[def.Name] = template.Must(template.New(def.Name).Parse(sqlT))
	}

	// open CSV reader iteratively
	f, err := os.Open("output/escooter_trips_simple.csv")
	if err != nil {
		log.Fatalf("open trips csv: %v", err)
	}
	defer f.Close()
	r := csv.NewReader(bufio.NewReader(f))
	headers, err := r.Read()
	if err != nil {
		log.Fatalf("read header: %v", err)
	}
	// index of each column
	idx := func(col string) int {
		for i, h := range headers {
			if h == col {
				return i
			}
		}
		log.Fatalf("column %q not found", col)
		return -1
	}
	iEventID := idx("event_id")
	iTripID := idx("trip_id")
	iTimestamp := idx("timestamp")
	iLat := idx("latitude")
	iLon := idx("longitude")

	// iterate rows
	count := 0
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("csv read error: %v", err)
		}
		// build params map
		params := map[string]interface{}{
			"EventID":   rec[iEventID],
			"TripID":    rec[iTripID],
			"Timestamp": rec[iTimestamp],
			"Longitude": rec[iLon],
			"Latitude":  rec[iLat],
		}
		// render and execute
		var buf io.Writer = os.Stdout // or a bytes.Buffer
		tmpls["InsertVehicleEvent"].Execute(buf, params)
		// TODO: actually run on db.Exec(...)
		count++
		// optionally measure per-N stats
		if count%10000 == 0 {
			log.Printf("inserted %d events so far...", count)
		}
	}
	log.Printf("finished inserting %d events in %v", count, time.Since(start))
}

func benchmarkQuery(db *sql.DB, target string, queries []QueryDef, districts []District, pois []POI) {
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
