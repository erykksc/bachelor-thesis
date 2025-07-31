-- --2) Display the geospatial trips in a reddable text format:
-- SELECT astext(trip):: varchar (100) AS trip
-- FROM trips
-- LIMIT 5;


--3) Display the individual observations composing the trip:
SELECT unnest (instants (trip)) AS instant
FROM trips
WHERE Vehicle = 1 AND day='2020-06-01' AND seq=1
LIMIT 10;

--4) Display the start and end timestamp of a trip:
SELECT startTimestamp(trip) AS start, endTimestamp (trip) AS end
FROM trips
LIMIT 5;

--5) What is the time span of the whole dataset ?
SELECT MIN(startTimestamp(trip)) AS begin, MAX(endTimestamp (trip)) AS end
FROM trips;

--6) What is the driven distance of every trip ?
SELECT length(trip)/1000 AS tripKms
FROM trips
LIMIT 5;

--7) Dataset summaries:
SELECT 
	MIN (length (trip)) AS minLength,
	MAX (length(trip)) AS maxLength,
	AVG (length(trip)) AS avgLength,
	MIN (duration (trip)) AS minDuration, MAX (duration(trip)) AS maxDuration,
	AVG (duration(trip)) AS avgDuration,
	MIN (numInstants (trip)) AS minPoints,
	MAX (numInstants(trip)) AS maxPoints,
	AVG (numInstants(trip)) AS avgPoints,
	AVG (numInstants(trip) * 60 / extract (epoch from timespan(trip))) AS avgPointsPerMinute
FROM trips;

--9) Compute the time-varying speed of the vehicles:
SELECT speed (trip):: varchar (100)
FROM trips
LIMIT 5;

--10) What is the average speed of the vehicles in Km/h:
SELECT twAvg(speed (trip)) * 3.6 AS averageSpeedKmH
FROM trips
LIMIT 5;

--11) (Temporal Aggregation) What is the count of vehicles at every instant between 16h00 and 19h00 in 20
SELECT tcount (
	atTime (trip, tstzspan '[2020-06-01 16:00:00, 2020-06-01 19:00:00]')) numTrips
FROM trips t
WHERE t.trip && tstzspan '[2020-06-01 16:00:00, 2020-06-01 19:00:00]';

-- unnest the aggregation into multiple rows
SELECT unnest (instants (tcount(
atTime (trip, tstzspan '[2020-06-01 16:00:00, 2020-06-01 19:00:00]')))) numTrips
FROM trips t
WHERE t.trip && tstzspan '[2020-06-01 16:00:00, 2020-06-01 19:00:00]';

--12) Compute the cumulative distance traveled by a vehicle as a temporal float:
SELECT
	cumulativeLength (trip)::varchar (100) AS cumulativeLength,
	endValue(cumulativeLength(trip)) AS totalLength,
	length(trip) AS totalLength2
FROM trips
LIMIT 5;

-- 14) (Spatial range and intersection) Which vehicle trips passed in the municipality of Evere
SELECT t.vehicle, t.day, t.seq
FROM trips t, communes c
WHERE c.name like '%Evere%' and intersects(t.trip, c.geom);

--15) (GiST and SP-GiST) Create a GiST/SP-GiST indexes on the trip attribute
drop index trip-gist;
CREATE INDEX trip-gist ON trips USING gist(trip) ;
CREATE INDEX trip_spgist ON trips USING spgist(trip);


SELECT t.vehicle, t.day, t.seq
FROM trips t, communes c
WHERE c.name like '%Evere%' and intersects(t.trip, c-geom);


--16) (Spatial KNN) What was the closest distance between any vehicle and 'Grand Place - Grote Markt'

SELECT MIN(trip |=| way) AS distance
FROM trips, planet_osm_point
WHERE name = 'Grand Place - Grote Markt';


--16) Which five vehicles came closest to 'Grand Place - Grote Markt' and what were their approach distan
SELECT t.vehicle, t.day, t.seq, (trip |=| way) AS distance
FROM trips t, planet_osm_point r
WHERE name = 'Grand Place - Grote Markt'
ORDER BY distance asc
LIMIT 5;

--16) How many trips start and finish in different municipalities ?
SELECT COUNT (*)
FROM trips T, communes S, communes E
WHERE S.id <> E.id AND
	ST_Intersects (startValue(T. trip), S.geom) AND
	ST_Intersects (endValue(T.trip), E.geom)
