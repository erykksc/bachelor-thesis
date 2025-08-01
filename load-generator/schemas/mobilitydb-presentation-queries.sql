-- 3) Display the individual observations composing the trip:
SELECT unnest(instants(trip)) AS instant
FROM trips
LIMIT 10;

-- 4) Display the start and end timestamp of a trip:
SELECT startTimestamp(trip) AS start, endTimestamp(trip) AS end
FROM trips
LIMIT 5;

-- 5) What is the time span of the whole dataset?
SELECT MIN(startTimestamp(trip)) AS begin, MAX(endTimestamp(trip)) AS end
FROM trips;

-- 6) What is the driven distance of every trip?
SELECT length(trip) / 1000 AS tripKms
FROM trips
LIMIT 5;

-- 7) Dataset summaries:
SELECT 
    MIN(length(trip)) AS minLength,
    MAX(length(trip)) AS maxLength,
    AVG(length(trip)) AS avgLength,
    MIN(duration(trip)) AS minDuration,
    MAX(duration(trip)) AS maxDuration,
    AVG(duration(trip)) AS avgDuration,
    MIN(numInstants(trip)) AS minPoints,
    MAX(numInstants(trip)) AS maxPoints,
    AVG(numInstants(trip)) AS avgPoints,
FROM trips;

-- 9) Compute the time-varying speed of the vehicles:
SELECT speed(trip)::varchar AS speed
FROM trips
LIMIT 5;

-- 10) What is the average speed of the vehicles in Km/h:
SELECT twAvg(speed(trip)) * 3.6 AS averageSpeedKmH
FROM trips
LIMIT 5;

-- 11) Temporal aggregation: count vehicles between 16h00 and 19h00
SELECT tcount(
  atTime(trip, tstzspan '[2020-06-01 16:00:00+00, 2020-06-01 19:00:00+00]')
) AS numTrips
FROM trips
WHERE trip && tstzspan '[2020-06-01 16:00:00+00, 2020-06-01 19:00:00+00]';

-- Unnest the aggregation into multiple rows:
SELECT unnest(instants(cnt)) AS numTrips
FROM (
  SELECT tcount(
    atTime(trip, tstzspan '[2020-06-01 16:00:00+00, 2020-06-01 19:00:00+00]')
  ) AS cnt
  FROM trips
  WHERE trip && tstzspan '[2020-06-01 16:00:00+00, 2020-06-01 19:00:00+00]'
) sub;

-- 12) Compute the cumulative distance traveled by a vehicle:
SELECT
  cumulativeLength(trip)::varchar AS cumulativeLength,
  endValue(cumulativeLength(trip)) AS totalLength,
  length(trip) AS totalLength2
FROM trips
LIMIT 5;

-- 14) Spatial range and intersection: which vehicle trips passed through Evere
SELECT t.*
FROM trips t
JOIN localities d
  ON tintersects(t.trip::tgeompoint, d.geo_shape) ?= true
WHERE d.name ILIKE '%Charlottenburg%';

-- 16a) Closest distance to 'Grand Place - Grote Markt'
SELECT MIN(trip |=| geo_point) AS distance
FROM trips, pois
WHERE name = 'Grand Place - Grote Markt';

-- 16b) Five closest trips to 'Grand Place - Grote Markt'
SELECT t.trip_id, t.trip |=| p.geo_point AS distance
FROM trips t, pois p
WHERE p.name = 'Charlottenburg'
ORDER BY distance ASC
LIMIT 5;

-- 16c) How many trips start and finish in different municipalities?
SELECT COUNT(*)
FROM trips T, localities S, localities E
WHERE S.locality_id <> E.locality_id
  AND ST_Intersects(startValue(T.trip), S.geo_shape)
  AND ST_Intersects(endValue(T.trip), E.geo_shape)
  AND T.trip && tstzspan '[2021-01-01 00:00:00, 2021-02-01 00:00:00)';
