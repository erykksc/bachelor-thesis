package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
)

func mustInitializeDb(ctx context.Context, connString string, dbTarget DBTarget, pois []POI, districts []District, ddl string) {
	logger.Info("Initializing Database", "databaseType", dbTarget, "connString", connString, "poiCount", len(pois), "districtCount", len(districts))

	// Choose Database specific insert methods
	insertPoiToDb := insertPoiToCratedb
	insertDistrictToDb := insertDistrictToCratedb
	switch dbTarget {
	case CrateDB:
		logger.Info("Initializing CrateDB")
		insertPoiToDb = insertPoiToCratedb
		insertDistrictToDb = insertDistrictToCratedb

	case MobilityDB:
		logger.Info("Initializing MobilityDB")
		insertPoiToDb = insertPoiToMobilitydb
		insertDistrictToDb = insertDistrictToMobilitydb
	}

	// Initialize database connection
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
		err := insertPoiToDb(ctx, conn, &poi)
		if err != nil {
			logger.Error("Error executing poi insert query", "error", err, "poiData", poi)
			os.Exit(1)
		}
		logger.Debug("Inserted POI", "poi", poi)
	}
	logger.Info("Inserted all POIs into database", "dbTarget", dbTarget, "poiCount", len(pois), "timeElapsed", time.Since(startTime))

	// Insert districts
	startTime = time.Now()
	for _, district := range districts {
		err := insertDistrictToDb(ctx, conn, &district)
		if err != nil {
			logger.Error("Error executing district insert query", "error", err, "districtData", district.String())
			os.Exit(1)
		}
		logger.Debug("Inserted District", "district", district.String())
	}
	logger.Info("Inserted all districts into database", "dbTarget", dbTarget, "districtCount", len(pois), "timeElapsed", time.Since(startTime))
}

func insertPoiToCratedb(ctx context.Context, conn *pgx.Conn, poi *POI) error {
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
	_, err := conn.Exec(ctx, query)
	return err
}

func insertPoiToMobilitydb(ctx context.Context, conn *pgx.Conn, poi *POI) error {
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
	_, err := conn.Exec(ctx, query)
	return err
}

func insertDistrictToCratedb(ctx context.Context, conn *pgx.Conn, district *District) error {
	query := `INSERT INTO districts( district_id, name, geo_shape)
				VALUES ( $1, $2, $3);`

	_, err := conn.Exec(ctx, query, district.DistrictID, district.Name, district.Geometry)
	return err
}

func insertDistrictToMobilitydb(ctx context.Context, conn *pgx.Conn, district *District) error {
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

	_, err := conn.Exec(ctx, query, district.DistrictID, district.Name, district.Geometry)
	return err
}
