# %% [markdown]
# # Escooter-Trip Simulation Notebook
# This notebook shows a simple pipeline to generate a large number of escooter trips in a German cities
# using OSMnx and NetworkX. Each cell is prefixed with `%%` so you can copy-paste directly into a Jupyter notebook.
# The trips are modeled using the following steps:
# 1. start point and start time of every ride is chosen randomly
# 2. end point and is chosen from points that are away of distance X
#   * Distance X is modeled with a right skewed distribution using log-normal distribution
# 3. The fastest route is computed using the OSMnx routing module which uses Dijkstra's algorithm
#   * The route is a list of nodes in a graph
# 4. The speed between the nodes along the trip is modeled with a uniform distribution between 10-20km/h (as 20 is max legal speed in germany)
# 5. Using the speed and the distance between the nodes, the time to travel between the nodes is computed
# 6. Using the route, timestart, and time to travel between nodes of the route, the single trips representing the position and time are generated
#   * those trips are representing the shared escooter reporting during trip
#   * those trips are exported in CSV format to "escooter_trips.csv"

# %%
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from os import process_cpu_count
from scipy.stats import lognorm, beta
from typing import List, Tuple
import logging
import networkx as nx
import numpy as np
import osmnx as ox
import pandas as pd
import random
import time
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
# Set the level, this allows to change the level without restarting the interpreter
logging.getLogger().setLevel(logging.INFO)


# optionally enable logging for progress
# ox.config(log_console=True, use_cache=True)

# %%
# 1. Load the bike network graph for Berlin (or your city of choice)
city = "Berlin, Germany"
logging.info(f"Loading bike network graph for {city}")
G = ox.graph_from_place(city, network_type="bike")
logging.info("Graph loaded, projecting to latlong")
G = ox.project_graph(G, to_latlong=True)
logging.info(f"Graph loaded: {len(G.nodes)} nodes, {len(G.edges)} edges")

# Add speed_kph attributes to edges, modelling the speed of escooters on them
# Model the speed using a beta distribution
logging.info("Adding speed_kph attribute to edges")
for u, v, k, data in G.edges(keys=True, data=True):
    # Parameters of the beta distribution
    alpha = 8
    beta_param = 1
    scooter_speed = float(beta.rvs(alpha, beta_param) * 20)  # scale to 0-20 km/h
    data["speed_kph"] = scooter_speed
logging.info("Speed_kph attribute added to edges")

logging.info("Adding travel times to edges")
G = ox.add_edge_travel_times(G)
logging.info("Travel times added to edges")


# %%
# 2. Sample OD pairs
# start_node is smapled randomly from the graph nodes,
# end_node is sampled as a node with distance modeled with right skewed distribution using log-normal distribution
def sample_od_pair(G, nodes, requested_trip_length) -> Tuple[int, int]:
    start_node = random.choice(nodes)
    nodes_within_radius = nx.ego_graph(
        G, start_node, center=True, radius=requested_trip_length, distance="length"
    )

    # Get boundary nodes (nodes at the edge of the ego graph)
    boundary_nodes = []
    for node in nodes_within_radius.nodes():
        if node != start_node:
            # Check if this node has neighbors outside the ego graph
            original_neighbors = set(G.neighbors(node))
            ego_neighbors = set(nodes_within_radius.neighbors(node))
            if original_neighbors - ego_neighbors:  # Has neighbors outside ego graph
                boundary_nodes.append(node)
    logging.debug(
        f"Found {len(boundary_nodes)} boundary nodes within radius {requested_trip_length} meters from start node {start_node}"
    )

    if not boundary_nodes:
        boundary_nodes = list(nodes_within_radius.nodes())
        boundary_nodes.remove(start_node)
        logging.warning(
            f"No boundary nodes found within radius {requested_trip_length} meters from start node {start_node}. Falling back to all nodes within radius (nodes: {len(boundary_nodes)})."
        )

    if not boundary_nodes:
        logging.warning(
            f"No valid end nodes found for start node {start_node} with requested trip length {requested_trip_length}m falling back to trip to the same node (start node)"
        )
        boundary_nodes = [start_node]

    end_node = random.choice(boundary_nodes)
    return (start_node, end_node)


num_trips = 20000
# Generate synthetic ride distances (meters)
requested_trip_lengths = np.round(
    lognorm.rvs(
        0.5,  # σ of the underlying normal
        loc=0,  # loc should be 0 for pure log-normal and as trips distances are positive
        scale=2100,  # median of the distribution
        size=num_trips,  # however many rides you want
    )
).astype(int)
requested_trip_lengths[0]

logging.info(f"Sampling {num_trips} OD pairs")
start_time = time.time()
nodes = list(G.nodes)
od_pairs: List[Tuple[int, int]] = []
for i, requested_trip_length in enumerate(requested_trip_lengths):
    if i % 100 == 0 and i > 0:
        logging.info(f"Sampled {i}/{num_trips} OD pairs")
    od_pair = sample_od_pair(G, nodes, requested_trip_length)
    od_pairs.append(od_pair)
end_time = time.time()
logging.info(f"Sampled {len(od_pairs)} OD pairs in {end_time - start_time:.2f} seconds")

# %%
# 4. Compute shortest-path routes (by length) for each OD pair
logging.info(
    f"Starting shortest path computation for {len(od_pairs)} OD pairs using multiprocessing"
)

routes: List[List[int]] = list()
route_lengths = list()
start_time = time.time()
for i, od_pair in enumerate(od_pairs):
    u, v = od_pair
    route = ox.routing.shortest_path(G, u, v, weight="length", cpus=process_cpu_count())
    if route == None:
        logging.warning(
            f"Route for OD pair {od_pair} could not be computed. Probably no route between them. Skipping."
        )
        continue

    routes.append(route)

    route_length = nx.path_weight(G, route, weight="length")
    route_lengths.append(route_length)
    logging.info(
        f"Computed route {i+1}/{len(od_pairs)} for OD pair {od_pair} with length {route_length}"
    )
end_time = time.time()

logging.info(
    f"Shortest path computation completed in {end_time - start_time:.2f} seconds"
)
logging.info(
    f"Computed {len(routes)} routes out of {num_trips} requested ({len(routes)/num_trips*100:.1f}% success rate)"
)

# %%
# 5. Create travel times, and by extension, trips

# create a trip from every route by adding the timestamps and creating multiple events
events = pd.DataFrame()
# Define timerange of the trips (when they can happen)
berlin_tz = ZoneInfo("Europe/Berlin")
start_ts = datetime(2020, 1, 1, tzinfo=berlin_tz).timestamp()
end_ts = datetime(2025, 1, 1, tzinfo=berlin_tz).timestamp()
for i, route in enumerate(routes):
    # go from one node to another until finishing the route, creating events along the way
    trip_id = uuid.uuid4()  # uuid
    u = route[0]  # start node
    u_idx = 0  # index of node u in route
    prev_u = -1  # none
    timestamp_at_u = datetime.now()  # declares the variable
    while True:
        if prev_u == -1:
            # Create a random timestamp within specified time range
            random_ts = random.uniform(start_ts, end_ts)
            random_dt = datetime.fromtimestamp(random_ts, tz=berlin_tz)
            timestamp_at_u = random_dt
        else:
            travel_time = float(G[prev_u][u][0]["travel_time"])  # pyright: ignore
            timestamp_at_u = timestamp_at_u + timedelta(seconds=travel_time)

        # create an event
        event = {
            "event_id": uuid.uuid4(),  # unique event ID
            "trip_id": trip_id,
            "timestamp": timestamp_at_u.isoformat(),
            "latitude": G.nodes[u]["y"],
            "longitude": G.nodes[u]["x"],
        }
        if not events.empty:
            events = pd.concat([events, pd.DataFrame([event])], ignore_index=True)
        else:
            events = pd.DataFrame([event])
            events.set_index("event_id", inplace=True)

        # if u not the last node in the route, move to the next node
        if u_idx < len(route) - 1:
            prev_u = u
            u_idx += 1
            u = route[u_idx]
        else:
            break

if not events.index.is_unique:
    logging.error(
        "Event IDs are not unique, this may cause issues in downstream processing."
    )

# Export to CSV
events.to_csv("output/escooter_trips.csv", index=False)
events.sort_values("timestamp").to_csv(
    "output/timestamp_sorted_escooter_trips.csv", index=False
)
logging.info(
    f"Exported {len(events)} events to escooter_trips.csv and timestamp_sorted_escooter_trips.csv"
)

# %%
# 6. (Optional) Visualize in-line
# fig, ax = ox.plot_graph_routes(G, routes, route_color="blue")
# fig.show()
# fig.savefig("route_example.png")
