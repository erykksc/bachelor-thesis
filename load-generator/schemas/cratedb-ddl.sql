CREATE TABLE IF NOT EXITS escooter_events (
    event_id TEXT PRIMARY KEY,
    trip_id TEXT,
    timestamp TIMESTAMP,
    geo_point GEO_POINT
)
CLUSTERED BY (trip_id) INTO 6 SHARDS
WITH ("number_of_replicas" = 1);

CREATE TABLE IF NOT EXISTS pois (
    poi_id TEXT PRIMARY KEY,
    name TEXT,
    category TEXT,
    geo_point GEO_POINT
)
CLUSTERED INTO 1 SHARDS
WITH ("number_of_replicas" = 'all');

CREATE TABLE IF NOT EXISTS districts (
    district_id TEXT PRIMARY KEY,
    name TEXT,
    geo_shape GEO_SHAPE
)
CLUSTERED INTO 1 SHARDS
WITH ("number_of_replicas" = 'all');
