DROP TABLE IF EXISTS escooter_events;
DROP TABLE IF EXISTS pois;
DROP TABLE IF EXISTS localities;

CREATE TABLE IF NOT EXISTS escooter_events (
    event_id  UUID PRIMARY KEY,
    trip_id   UUID,
    timestamp TIMESTAMPTZ,
    geo_point geometry(Point, 4326)
);

CREATE INDEX IF NOT EXISTS escooter_events_timestamp_idx   ON escooter_events (timestamp);

CREATE TABLE IF NOT EXISTS trips (
    trip_id         UUID PRIMARY KEY,
    trip            tgeogpoint
);

CREATE INDEX IF NOT EXISTS trips_trip_gist   ON trips USING GIST (trip);
CREATE INDEX IF NOT EXISTS trips_trip_spgist ON trips USING SPGIST (trip);

CREATE TABLE IF NOT EXISTS pois (
    poi_id    UUID PRIMARY KEY,
    name      TEXT,
    category  TEXT,
    geo_point geometry(Point, 4326)
);

CREATE INDEX IF NOT EXISTS pois_geo_point_gist        ON pois      USING GIST (geo_point);
CREATE INDEX IF NOT EXISTS pois_geo_point_spgist      ON pois      USING SPGIST (geo_point);

CREATE TABLE IF NOT EXISTS localities (
    locality_id UUID PRIMARY KEY,
    name        TEXT,
    geo_shape   geometry(MultiPolygon, 4326)
);


CREATE INDEX IF NOT EXISTS localities_geo_shape_gist   ON localities USING GIST (geo_shape);
CREATE INDEX IF NOT EXISTS localities_geo_shape_spgist ON localities USING SPGIST (geo_shape);

-- Replicate small tables to every worker (fast local joins, no broadcast)
SELECT create_reference_table('pois');
SELECT create_reference_table('localities');

-- Distribute by trip_id (hash), keep rows of same trip together
SELECT create_distributed_table(
    'trips',
    'trip_id',
    'hash',
    shard_count => 32,
    colocate_with => 'none'
);
