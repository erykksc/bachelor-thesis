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
	logger.Info("Initializing Database", "databaseType", dbTarget.String(), "connString", connString, "poiCount", len(pois), "districtCount", len(districts))

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

		_, err = conn.Exec(ctx, string(migrationSQL))
		if err != nil {
			logger.Error("Error executing migration", "migrationFile", migrationFile, "error", err)
			os.Exit(1)
		}

		logger.Info("Migration completed successfully", "file", migrationFile)
	}

	// Insert POIs
	startTime := time.Now()
	switch dbTarget {
	case CrateDB:
		err = insertPoisToCratedb(ctx, conn, pois)

	case MobilityDB:
		err = insertPoisToMobilitydb(ctx, conn, pois)
	}
	if err != nil {
		logger.Error("Error inserting POIs into database", "dbTarget", dbTarget.String(), "error", err)
		os.Exit(1)
	}
	logger.Info("Inserted all POIs into database", "dbTarget", dbTarget.String(), "poiCount", len(pois), "timeElapsedInSec", time.Since(startTime).Seconds())

	// Insert districts

	// Choose Database specific insert methods
	queueDistrictInsert := queueDistrictInsertToCratedb
	switch dbTarget {
	case CrateDB:
		queueDistrictInsert = queueDistrictInsertToCratedb
	case MobilityDB:
		queueDistrictInsert = queueDistrictInsertToMobilitydb
	}

	startTime = time.Now()
	pgxBatch := &pgx.Batch{}
	for _, district := range districts {
		queueDistrictInsert(pgxBatch, &district)
	}
	batchResults := conn.SendBatch(ctx, pgxBatch)
	defer batchResults.Close()
	for _, district := range districts {
		_, err := batchResults.Exec()
		if err != nil {
			logger.Error("Error executing district insert query", "error", err, "districtData", district.String())
			os.Exit(1)
		}
	}
	batchResults.Close()
	logger.Info("Inserted all districts into database", "dbTarget", dbTarget.String(), "districtCount", len(districts), "timeElapsedInSec", time.Since(startTime).Seconds())
}

func insertPoisToCratedb(ctx context.Context, conn *pgx.Conn, pois []POI) error {
	poiIds := make([]string, len(pois))
	names := make([]string, len(pois))
	categories := make([]string, len(pois))
	geo_points := make([]string, len(pois))
	for i, poi := range pois {
		poiIds[i] = poi.POIID
		names[i] = poi.Name
		categories[i] = poi.Category
		geo_points[i] = fmt.Sprintf("POINT( %s %s )", poi.Longitude, poi.Latitude)
	}

	query := fmt.Sprintf(`
	INSERT INTO pois ( 
		poi_id,
		name,
		category,
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
		joinAndQuoteStrings(poiIds),
		joinAndQuoteStrings(names),
		joinAndQuoteStrings(categories),
		joinAndQuoteStrings(geo_points),
	)

	_, err := conn.Exec(ctx, query)
	return err
}

func insertPoisToMobilitydb(ctx context.Context, conn *pgx.Conn, pois []POI) error {
	poiIds := make([]string, len(pois))
	names := make([]string, len(pois))
	categories := make([]string, len(pois))
	geo_points := make([]string, len(pois))
	for i, poi := range pois {
		poiIds[i] = poi.POIID
		names[i] = poi.Name
		categories[i] = poi.Category
		geo_points[i] = fmt.Sprintf("ST_SetSRID(ST_MakePoint(%s, %s), 4326)", poi.Longitude, poi.Latitude)
	}

	query := fmt.Sprintf(`
	INSERT INTO pois ( 
		poi_id,
		name,
		category,
		geo_point
	)
	(SELECT *
		FROM  UNNEST(
		ARRAY[%s]::UUID[],
		ARRAY[%s],
		ARRAY[%s],
		ARRAY[%s]::geometry(Point, 4326)[]
		)
	);`,
		joinAndQuoteStrings(poiIds),
		joinAndQuoteStrings(names),
		joinAndQuoteStrings(categories),
		strings.Join(geo_points, ","),
	)

	_, err := conn.Exec(ctx, query)
	return err
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
		`INSERT INTO districts ( district_id, name, geo_shape)
		VALUES ( $1, $2, ST_GeomFromGeoJSON($3));`,
		district.DistrictID, district.Name, district.Geometry)
}
