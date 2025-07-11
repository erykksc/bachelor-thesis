CREATE TABLE escooter_events (
    event_id UUID PRIMARY KEY,
    trip_id UUID,
    timestamp TIMESTAMP,
    geo_point GEO_POINT
);

CREATE TABLE pois (
    poi_id UUID PRIMARY KEY,
    name TEXT,
    category TEXT,
    geo_point GEO_POINT
);

CREATE TABLE districts (
    district_id UUID PRIMARY KEY,
    name TEXT,
    geo_shape GEO_SHAPE
);
