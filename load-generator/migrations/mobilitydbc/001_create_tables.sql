DROP TABLE IF EXISTS escooter_events;
DROP TABLE IF EXISTS pois;
DROP TABLE IF EXISTS districts;

CREATE TABLE IF NOT EXISTS escooter_events (
    event_id   UUID PRIMARY KEY,
    trip_id    UUID,
    timestamp  TIMESTAMP,
    tgeo_point tgeogpoint
);

-- Distribute by trip_id (hash), keep rows of same trip together
SELECT create_distributed_table(
    'escooter_events',
    'trip_id',
    'hash',
    shard_count => 32,
    colocate_with => NULL
);

CREATE INDEX CONCURRENTLY IF NOT EXISTS escooter_events_ts_idx            ON escooter_events ("timestamp");
CREATE INDEX CONCURRENTLY IF NOT EXISTS escooter_events_tgeo_point_gist   ON escooter_events USING GIST (tgeo_point);
CREATE INDEX CONCURRENTLY IF NOT EXISTS escooter_events_tgeo_point_spgist ON escooter_events USING SPGIST (tgeo_point);

CREATE TABLE IF NOT EXISTS pois (
    poi_id    UUID PRIMARY KEY,
    name      TEXT,
    category  TEXT,
    geo_point geometry(Point, 4326)
);

CREATE TABLE IF NOT EXISTS districts (
    district_id UUID PRIMARY KEY,
    name        TEXT,
    geo_shape   geometry(MultiPolygon, 4326)
);

-- Replicate small tables to every worker (fast local joins, no broadcast)
SELECT create_reference_table('pois');
SELECT create_reference_table('districts');

-- Spatial indexes
CREATE INDEX IF NOT EXISTS pois_geo_point_gist        ON pois      USING GIST (geo_point);
CREATE INDEX IF NOT EXISTS pois_geo_point_spgist      ON pois      USING SPGIST (geo_point);
CREATE INDEX IF NOT EXISTS districts_geo_shape_gist   ON districts USING GIST (geo_shape);
CREATE INDEX IF NOT EXISTS districts_geo_shape_spgist ON districts USING SPGIST (geo_shape);
