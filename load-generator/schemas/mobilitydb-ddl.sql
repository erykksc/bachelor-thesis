CREATE TABLE escooter_events (
    event_id UUID PRIMARY KEY,
    trip_id UUID,
    timestamp TIMESTAMP,
    geom geometry(Point, 4326)
);

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
