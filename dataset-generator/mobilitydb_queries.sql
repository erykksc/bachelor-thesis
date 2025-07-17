-- 1. Spatiotemporal Range
-- Vehicles in a specific polygon between 8–10 AM
SELECT *
FROM vehicle_trips
WHERE trip && stbox('POLYGON((...))', '[2025-06-21 08:00, 2025-06-21 10:00]');


-- 2. Proximity + Time
-- Vehicles within 500m of a point between 6–8 PM
SELECT *
FROM vehicle_trips
WHERE dwithin(trip, ST_Point(13.405, 52.52), 500)
  AND trip @ '[2025-06-21 18:00, 2025-06-21 20:00]';


-- 3. Movement Tracking
-- Retrieve full trajectory of a specific truck for a day
SELECT trip
FROM vehicle_trips
WHERE vehicle_id = 'truck_123'
  AND trip @ '[2025-06-21, 2025-06-21]';


-- 4. Time-Bucketed Aggregation by Location
-- Average speed per zone over time
SELECT zone_id, temporal_avg(twAvg(speed))
FROM vehicle_trips
GROUP BY zone_id;


-- 5. Spatiotemporal Joins
-- Events and vehicles that overlap in space and time
SELECT a.vehicle_id, b.event_id
FROM vehicle_trips a
JOIN city_events b
  ON intersects(a.trip, b.geom)
  AND a.trip @ b.period;


-- 6. Nearest Object at Time
-- Closest vehicle to a location at 12:30 PM
SELECT vehicle_id
FROM vehicle_trips
ORDER BY distance(valueAtTimestamp(trip, '2025-06-21 12:30:00'), ST_Point(13.405, 52.52))
LIMIT 1;


-- 7. Cumulative Spatiotemporal Metrics
-- Total distance traveled per vehicle in the past week
SELECT vehicle_id, tsum(length(trip)) AS total_distance
FROM vehicle_trips
WHERE trip @ '[2025-06-17, 2025-06-23]'
GROUP BY vehicle_id;


-- 8. Co-location Events
-- Pairs of vehicles within 100m at any time
SELECT a.vehicle_id, b.vehicle_id
FROM vehicle_trips a, vehicle_trips b
WHERE a.vehicle_id < b.vehicle_id
  AND tdwithin(a.trip, b.trip, 100);


-- 9. Area Dwell Time
-- Duration vehicles spent in a specific zone
SELECT vehicle_id, duration(atGeometry(trip, ST_GeomFromText('POLYGON((...))')))
FROM vehicle_trips;


-- 10. Movement Pattern Detection
-- Detect direction changes using azimuth
SELECT vehicle_id, azimuth(trip) AS direction_changes
FROM vehicle_trips;


-- 11. Gap Detection
-- Find GPS gaps longer than 5 minutes
SELECT vehicle_id, interp
FROM (
  SELECT vehicle_id, interpolation(trip) AS interp
  FROM vehicle_trips
) AS gaps
WHERE duration(interp) > interval '5 minutes';


-- 12. Real-time Event Monitoring
-- Vehicles currently inside a defined zone
SELECT vehicle_id
FROM vehicle_trips
WHERE atPeriod(trip, period(now(), now())) && ST_GeomFromText('POLYGON((...))');
