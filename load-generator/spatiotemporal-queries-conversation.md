## Types of Queries Supported by CrateDB for Spatiotemporal Data

CrateDB is designed to efficiently handle and query data that includes both spatial (location) and temporal (time) components.
Below is a summary of the main types of queries and features relevant to spatiotemporal data:

**Spatial (Geospatial) Queries**

- **Data Types**: CrateDB supports `GEO_POINT` for latitude/longitude coordinates and `GEO_SHAPE` for more complex geometries like polygons, lines, and multi-shapes[^1_1][^1_2].
- **Spatial Functions**:
  - **within**: Checks if a point or shape is within a specified area (e.g., "find all records within a city boundary")[^1_3][^1_2].
  - **distance**: Calculates the distance between two points, useful for proximity searches (e.g., "find the closest sensor to this location")[^1_3][^1_2].
  - **intersects**: Determines if two shapes overlap (e.g., "find all delivery zones that intersect with a given route")[^1_2].
- **Spatial Filtering**: Queries can filter results by bounding boxes, circles, or arbitrary shapes, supporting operations like "find all objects within 5 km of a point" or "find all points inside a polygon"[^1_1].
- **Geospatial Indexing**: CrateDB leverages advanced index structures (Prefix Tree, BKD-tree) for fast spatial queries[^1_1].

**Temporal (Time Series) Queries**

- **Time-based Partitioning**: CrateDB natively supports partitioning and sharding by time, enabling efficient storage and retrieval of large time series datasets[^1_4].
- **Window and Aggregation Functions**: Functions like `MAX_BY`, `MIN_BY`, `LEAD`, `LAG`, and `DATE_BIN` allow for advanced time-based analytics, such as finding trends, gaps, or changes over time[^1_4][^1_5].
- **Time Bucketing**: Group and aggregate data into fixed time intervals (e.g., 10-second buckets), which is crucial for analyzing trends or summarizing activity over time[^1_5].
- **Gap Filling and Interpolation**: Use window functions and CTEs to fill missing time intervals or interpolate values in time series data[^1_5].

**Combined Spatiotemporal Queries**

CrateDB's SQL engine allows combining spatial and temporal conditions in a single query. For example:

- "Find all vehicles that were within a certain area during a specific time window."
- "Aggregate sensor readings by location and hourly intervals."
- "Track the movement of an object over time and space."

**Integration and Compatibility**

- **GeoJSON Support**: CrateDB can ingest and query data formatted as GeoJSON, making it compatible with common geospatial standards and tools[^1_6][^1_1].
- **Real-time Analytics**: The combination of spatial and temporal query support enables real-time analytics for use cases like fleet tracking, IoT sensor monitoring, and smart city applications[^1_6][^1_4].

## Summary Table

| Query Type        | Example Use Case                    | CrateDB Features/Functions               |
| :---------------- | :---------------------------------- | :--------------------------------------- |
| Spatial Filtering | Find all points within a polygon    | `within`, `distance`, `intersects`       |
| Proximity Search  | Find nearest sensors to a location  | `distance`                               |
| Time Series Query | Aggregate readings per minute       | `DATE_BIN`, `GROUP BY`, window functions |
| Spatiotemporal    | Find events in area X during time Y | Combine spatial + temporal WHERE clauses |
| Trend Analysis    | Track movement over time and space  | Time bucketing, window functions         |

## Conclusion

CrateDB supports a robust set of SQL-based queries for spatiotemporal data, including:

- Geospatial queries using `within`, `distance`, and `intersects`
- Time series queries with advanced aggregation, partitioning, and interpolation
- Combined queries that filter and analyze data by both location and time

These capabilities make CrateDB suitable for real-time analytics in domains such as IoT, logistics, and smart cities[^1_6][^1_4][^1_2].

<div style="text-align: center">⁂</div>

[^1_1]: https://cratedb.com/docs/guide/feature/search/geo/index.html

[^1_2]: https://cratedb.com/data-model/geospatial

[^1_3]: https://cratedb.com/blog/geospatial-queries-with-crate-data

[^1_4]: https://cratedb.com/data-model/time-series/time-series-data-in-cratedb

[^1_5]: https://cratedb.com/docs/guide/feature/query/index.html

[^1_6]: https://cratedb.com/use-cases/geospatial-analytics

[^1_7]: https://cratedb.com/docs/crate/reference/en/latest/general/dql/geo.html

[^1_8]: https://ngsi-ld-tutorials.readthedocs.io/en/latest/time-series-data.html

[^1_9]: https://openproceedings.org/2021/conf/edbt/p162.pdf

[^1_10]: https://cratedb.com/blog/time-series-cratedb-cloud-sql-examples

---

## Types of Queries Supported by MobilityDB for Spatiotemporal Data

MobilityDB is an advanced PostgreSQL extension that specializes in managing and querying spatiotemporal (location and time) data, particularly for moving objects such as vehicles, ships, or people.
Its query capabilities are broad, leveraging both spatial and temporal dimensions, often simultaneously.

**Key Query Types**

- **Range Queries**
  - Restrict results based on spatial, temporal, or spatiotemporal ranges.
  - Examples:
    - Find all vehicles that passed through a specific region.
    - List vehicles within a region during a specific time period.
    - Identify pairs of vehicles that were both in a region during a time window[^2_1].
- **Distance Queries**
  - Find objects within a certain distance of each other, in space or time.
  - Examples:
    - List pairs of vehicles that were within 10 meters of each other at any time.
    - Find ships that came closer than 300 meters to one another[^2_1][^2_2].
- **Temporal Aggregate Queries**
  - Aggregate data over time, such as calculating cumulative distances, average speeds, or other statistics.
  - Examples:
    - Compute the cumulative distance traveled by a fleet over a week.
    - Calculate time-weighted averages (e.g., average speed over a period)[^2_2].
- **Nearest-Neighbor Queries**
  - Identify the closest objects in space and/or time.
  - Example:
    - Find the nearest vehicle to a given location at a particular time[^2_1].

**Spatiotemporal Functions and Operators**

- MobilityDB generalizes PostGIS spatial functions to the spatiotemporal domain, allowing operations like `ST_Intersection`, `dwithin`, and `intersects` to be applied to moving objects over time[^2_3].
- Temporal functions include restricting data to specific time periods (`atperiods`), extracting durations, and analyzing temporal patterns[^2_3].
- Spatiotemporal functions can compute speed, azimuth (direction), maximum/minimum values, and time-weighted averages for moving objects[^2_3].

**Indexing and Performance**

- MobilityDB extends PostgreSQL's GiST and SP-GiST indexes for efficient querying of spatiotemporal data, supporting R-tree, TB-tree, Quad-tree, and Oct-tree structures for various data types[^2_3].
- These indexes enable high-performance queries on large datasets of moving objects.

**Distributed Query Support**

- Distributed MobilityDB extends these capabilities to clusters, allowing spatial-only, temporal-only, and spatiotemporal queries to be executed in a distributed environment for scalability[^2_4][^2_5].

## Summary Table

| Query Type           | Example Use Case                             | Functions/Operators                    |
| :------------------- | :------------------------------------------- | :------------------------------------- |
| Spatial Range        | Vehicles passing through a region            | `ST_Intersects`, `&&`                  |
| Spatiotemporal Range | Vehicles in region during time window        | `atPeriod`, `intersects`, `stbox`      |
| Distance/Proximity   | Vehicles within 10m of each other            | `tdwithin`, `expandSpatial`            |
| Temporal Aggregation | Cumulative distance, average speed over time | `tsum`, `twAvg`, `maxValue`            |
| Nearest Neighbor     | Closest vehicle at a given time              | `dwithin`, `shortestLine`              |
| Spatiotemporal Join  | Pairs of objects close in space and time     | `dwithin`, `intersects`, `atPeriodSet` |

## Conclusion

MobilityDB supports a comprehensive set of spatiotemporal queries, including range, distance, aggregation, and nearest-neighbor queries.
It extends spatial SQL with temporal and spatiotemporal logic, providing powerful tools for analyzing moving objects and their trajectories in both space and time[^2_1][^2_3][^2_2].

<div style="text-align: center">⁂</div>

[^2_1]: https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch01s05.html

[^2_2]: https://docs.mobilitydb.com/pub/MobilityDB-PGDay-2020.pdf

[^2_3]: https://pgconf.ru/en/2019/242944

[^2_4]: https://github.com/mbakli/DistributedMobilityDB

[^2_5]: https://dipot.ulb.ac.be/dspace/bitstream/2013/330089/3/DistMobilityDB_BigSpatial19.pdf

[^2_6]: https://dl.acm.org/doi/10.1145/3719202

[^2_7]: https://docs.mobilitydb.com/pub/MobilityDBDemo_MDM2020.pdf

[^2_8]: https://docs.mobilitydb.com/MobilityDB/develop/

[^2_9]: https://docs.mobilitydb.com/pub/MobilityDB_PGConf2021_Tutorial.pdf

[^2_10]: https://mobilitydb.com/project.html

---

## Spatiotemporal Queries Supported by Both CrateDB and MobilityDB

Based on documented capabilities, both databases support the following spatiotemporal query types:

1. **Spatial Range Queries**
   Retrieve objects within a geographic area - _Example_: "Find all sensors in a city boundary" - **CrateDB**: `WHERE ST_Within(location, polygon)` [^3_1] - **MobilityDB**: `WHERE ST_Intersects(trajectory, geom)` [^3_5]
2. **Spatiotemporal Range Queries**
   Filter objects within a geographic area during a specific time window - _Example_: "Find vehicles in Berlin between 7-9 AM" - **CrateDB**: `WHERE within(geo_point, area) AND timestamp BETWEEN ...` [^3_2][^3_3] - **MobilityDB**: `WHERE trip && stbox(geom, period) AND intersects(...)` [^3_5][^3_6]
3. **Proximity/Distance Queries**
   Identify objects near a location - _Example_: "Find sensors within 500m of a point" - **CrateDB**: `WHERE distance(geo_point, target) < 500` [^3_1] - **MobilityDB**: `WHERE dwithin(trip, point, 500)` [^3_5][^3_10]
4. **Time-Based Aggregations**
   Summarize data over time intervals - _Example_: "Hourly average speed per vehicle" - **CrateDB**: `DATE_BIN('1 hour', timestamp) GROUP BY ...` [^3_2] - **MobilityDB**: `temporal_agg(twAvg(speed))` [^3_10][^3_6]
5. **Temporal Filtering**
   Retrieve object states at specific times - _Example_: "Vehicle locations at 08:00 AM" - **CrateDB**: `WHERE timestamp = '2025-06-21 08:00'` [^3_2] - **MobilityDB**: `valueAtTimestamp(trip, '2025-06-21 08:00')` [^3_9]
6. **Spatiotemporal Joins**
   Combine datasets based on location/time overlap - _Example_: "Match delivery trucks to warehouses during operating hours" - **CrateDB**: Spatial join + time filter in `WHERE` [^3_1][^3_2] - **MobilityDB**: `JOIN ... ON intersects(trip, geom) AND overlaps(period)` [^3_5][^3_6]

### Key Differences

- **Moving Objects**: MobilityDB specializes in trajectory analysis (e.g., `tgeompoint` for continuous movement) [^3_6][^3_9], while CrateDB focuses on discrete spatial points over time [^3_2][^3_3].
- **Indexing**: MobilityDB uses GiST/SP-GiST for trajectory indexing [^3_6][^3_7], whereas CrateDB uses BKD-trees for geospatial data [^3_1].
- **Distribution**: CrateDB natively supports distributed queries [^3_2][^3_4], while MobilityDB requires Citus for distribution [^3_11][^3_7].

Both enable SQL-based analysis of location and time data, but MobilityDB offers deeper temporal semantics for moving objects, while CrateDB excels at scalable time-series/geospatial hybrid workloads.

<div style="text-align: center">⁂</div>

[^3_1]: https://cratedb.com/data-model/geospatial

[^3_2]: https://cratedb.com/docs/guide/feature/query/index.html

[^3_3]: https://www.perplexity.ai/search/e755441b-f21d-45cc-acd2-6d271a2e2fff

[^3_4]: https://www.perplexity.ai/search/4f3af5e5-37f1-4de6-999a-f0505ed9b212

[^3_5]: https://docs.mobilitydb.com/MobilityDB-BerlinMOD/master/ch01s05.html

[^3_6]: https://docs.mobilitydb.com/pub/SigSpatial2020.pdf

[^3_7]: https://dipot.ulb.ac.be/dspace/bitstream/2013/330089/3/DistMobilityDB_BigSpatial19.pdf

[^3_8]: https://www.perplexity.ai/search/9f9097a5-0a24-4415-b694-81ca3c0d322e

[^3_9]: https://www.perplexity.ai/search/afe3fb15-ad24-41a1-9cfc-bd4aae878954

[^3_10]: https://github.com/MobilityDB/meos-rs

[^3_11]: https://docs.mobilitydb.com/pub/MobilityDB-PGDay-2020.pdf

[^3_12]: https://community.cratedb.com/t/fetching-large-result-sets-from-cratedb/1270

[^3_13]: https://www.youtube.com/watch?v=XXsZPxN-fcA

[^3_14]: https://community.cratedb.com/t/postgresql-time-related-queries-faster-than-the-same-queries-in-cratedb/921
