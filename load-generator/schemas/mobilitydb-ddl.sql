CREATE EXTENSION IF NOT EXISTS citus;
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS mobilitydb CASCADE;

CREATE TABLE escooter_events (
    event_id UUID PRIMARY KEY,
    trip_id UUID,
    timestamp TIMESTAMP,
    location tgeompoint
);


-- Distribute by trip_id (hash); keep rows of same trip together
SELECT create_distributed_table(
    'escooter_events',
    'trip_id',
    'hash',
    shard_count => 32,          -- <== change for your tests: 3, 6, 12, 32...
    colocate_with => NULL       -- set to another table if you want co-location
);

-- Indexes (GLOBAL syntax will create on all shards)
CREATE INDEX CONCURRENTLY IF NOT EXISTS escooter_events_ts_idx ON escooter_events ("timestamp");
CREATE INDEX CONCURRENTLY IF NOT EXISTS escooter_events_loc_gist ON escooter_events USING GIST (location);

CREATE TABLE pois (
    poi_id UUID PRIMARY KEY,
    name TEXT,
    category TEXT,
    geom geometry(Point, 4326)
);

CREATE TABLE districts (
    district_id UUID PRIMARY KEY,
    name TEXT,
    geom geometry(MultiPolygon, 4326)
);

-- Replicate to every worker (fast local joins, no broadcast)
SELECT create_reference_table('pois');
SELECT create_reference_table('districts');

-- Spatial indexes
CREATE INDEX IF NOT EXISTS pois_geom_gist       ON pois      USING GIST (geom);
CREATE INDEX IF NOT EXISTS districts_geom_gist  ON districts USING GIST (geom);
