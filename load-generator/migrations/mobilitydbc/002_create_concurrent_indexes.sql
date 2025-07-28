-- Indexes (GLOBAL syntax will create on all shards)
CREATE INDEX CONCURRENTLY IF NOT EXISTS escooter_events_ts_idx ON escooter_events ("timestamp");
CREATE INDEX CONCURRENTLY IF NOT EXISTS escooter_events_loc_gist ON escooter_events USING GIST (location);