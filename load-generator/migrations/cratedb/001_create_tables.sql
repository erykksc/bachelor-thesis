DROP TABLE IF EXISTS escooter_events;
DROP TABLE IF EXISTS pois;
DROP TABLE IF EXISTS districts;

CREATE TABLE IF NOT EXISTS escooter_events (
    event_id    TEXT,
    trip_id     TEXT,
    timestamp   TIMESTAMP,
    geo_point   GEO_POINT,
    PRIMARY KEY (trip_id, event_id)
)
CLUSTERED BY (trip_id) INTO 2 SHARDS
WITH ("number_of_replicas" = 0);

CREATE TABLE IF NOT EXISTS pois (
    poi_id    TEXT PRIMARY KEY,
    name      TEXT,
    category  TEXT,
    geo_point GEO_POINT
)
CLUSTERED INTO 1 SHARDS
WITH ("number_of_replicas" = '0-all');

CREATE TABLE IF NOT EXISTS districts (
    district_id TEXT PRIMARY KEY,
    name        TEXT,
    geo_shape   GEO_SHAPE
)
CLUSTERED INTO 1 SHARDS
WITH ("number_of_replicas" = '0-all');
