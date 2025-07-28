package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func mustInitializeDb(ctx context.Context, connString string, dbTarget DBTarget, pois []POI, districts []District, migrationsDir string) {
	logger.Info("Initializing Database", "databaseType", dbTarget, "connString", connString, "poiCount", len(pois), "districtCount", len(districts))

	// Choose Database specific insert methods
	queuePoiInsert := queuePoiInsertToCratedb
	queueDistrictInsert := queueDistrictInsertToCratedb
	switch dbTarget {
	case CrateDB:
		logger.Info("Initializing CrateDB")
		queuePoiInsert = queuePoiInsertToCratedb
		queueDistrictInsert = queueDistrictInsertToCratedb

	case MobilityDB:
		logger.Info("Initializing MobilityDB")
		queuePoiInsert = queuePoiInsertToMobilitydb
		queueDistrictInsert = queueDistrictInsertToMobilitydb
	}

	// Initialize database connection
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		logger.Error("Unable to connect to database", "error", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)
	logger.Info("Connected to database", "db", dbTarget)

	// Run migrations
	// Get all migration files sorted by name
	migrationFiles, err := filepath.Glob(filepath.Join(migrationsDir, "*.sql"))
	if err != nil {
		logger.Error("Error reading migration files", "error", err)
		os.Exit(1)
	}
	sort.Strings(migrationFiles)

	// Execute each migration file
	for _, migrationFile := range migrationFiles {
		logger.Info("Running migration", "file", migrationFile)
		migrationSQL, err := os.ReadFile(migrationFile)
		if err != nil {
			logger.Error("Error reading migration file", "file", migrationFile, "error", err)
			os.Exit(1)
		}

		statements := strings.SplitSeq(string(migrationSQL), ";")
		for stmt := range statements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			if _, err := conn.Exec(ctx, stmt); err != nil {
				logger.Error("Error executing migration", "file", migrationFile, "error", err)
				os.Exit(1)
			}
		}
		logger.Info("Migration completed successfully", "file", migrationFile)
	}

	// Insert POIs
	startTime := time.Now()
	pgxBatch := &pgx.Batch{}
	for _, poi := range pois {
		queuePoiInsert(pgxBatch, &poi)
	}
	batchResults := conn.SendBatch(ctx, pgxBatch)
	defer batchResults.Close()
	for _, poi := range pois {
		_, err := batchResults.Exec()
		if err != nil {
			logger.Error("Error executing poi insert query", "error", err, "poiData", poi)
			os.Exit(1)
		}
	}
	batchResults.Close()
	logger.Info("Inserted all POIs into database", "dbTarget", dbTarget, "poiCount", len(pois), "timeElapsedInSec", time.Since(startTime).Seconds())

	// Insert districts
	startTime = time.Now()
	pgxBatch = &pgx.Batch{}
	for _, district := range districts {
		queueDistrictInsert(pgxBatch, &district)
	}
	batchResults = conn.SendBatch(ctx, pgxBatch)
	defer batchResults.Close()
	for _, district := range districts {
		_, err := batchResults.Exec()
		if err != nil {
			logger.Error("Error executing district insert query", "error", err, "districtData", district.String())
			os.Exit(1)
		}
	}
	batchResults.Close()
	logger.Info("Inserted all districts into database", "dbTarget", dbTarget, "districtCount", len(districts), "timeElapsedInSec", time.Since(startTime).Seconds())
}

func queuePoiInsertToCratedb(batch *pgx.Batch, poi *POI) *pgx.QueuedQuery {
	// string interpolation is done instead of passing arguments to conn.Exec
	// as I want to avoid istalling additional library to support GEO_POINT types
	query := fmt.Sprintf(
		`INSERT INTO pois( poi_id, name, category, geo_point)
		VALUES ( '%s', '%s', '%s', [%s, %s]);`,
		poi.POIID, poi.Name, poi.Category, poi.Latitude, poi.Longitude)
	logger.Debug("queuing POI insert", "sql", query, "poi", poi)
	return batch.Queue(query)
}

func queuePoiInsertToMobilitydb(batch *pgx.Batch, poi *POI) *pgx.QueuedQuery {
	query := fmt.Sprintf(
		`INSERT INTO pois ( poi_id, name, category, geom)
		VALUES ( '%s', '%s', '%s', ST_SetSRID(ST_MakePoint(%s, %s), 4326));`,
		poi.POIID, poi.Name, poi.Category, poi.Longitude, poi.Latitude)

	logger.Debug("queuing POI insert", "sql", query, "poi", poi)
	return batch.Queue(query)
}

func queueDistrictInsertToCratedb(batch *pgx.Batch, district *District) *pgx.QueuedQuery {
	return batch.Queue(
		`INSERT INTO districts( district_id, name, geo_shape)
		VALUES ( $1, $2, $3);`,
		district.DistrictID, district.Name, district.Geometry,
	)
}

func queueDistrictInsertToMobilitydb(batch *pgx.Batch, district *District) *pgx.QueuedQuery {
	return batch.Queue(
		`INSERT INTO districts ( district_id, name, geom)
		VALUES ( $1, $2, ST_GeomFromGeoJSON($3));`,
		district.DistrictID, district.Name, district.Geometry)
}
