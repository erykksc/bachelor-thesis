-- Enable MobilityDB extension
CREATE EXTENSION IF NOT EXISTS mobilitydb CASCADE;

-- Create table for escooter events using temporal geometry points
CREATE TABLE escooter_events (
    event_id UUID PRIMARY KEY,
    trip_id UUID,
    timestamp TIMESTAMP,
    location tgeompoint
);

-- Create table for points of interest
CREATE TABLE pois (
    poi_id UUID PRIMARY KEY,
    name TEXT,
    category TEXT,
    geom geometry(Point, 4326)
);

-- Create table for districts
CREATE TABLE districts (
    district_id UUID PRIMARY KEY,
    name TEXT,
    geom geometry(MultiPolygon, 4326)
);
