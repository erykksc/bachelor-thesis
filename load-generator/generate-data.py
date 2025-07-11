import osmnx as ox
import networkx as nx
import pandas as pd
import random
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 1. Download a street network for your area of interest
#    e.g. the city of “Cambridge, UK”
city = "Berlin, Germany"
G = ox.graph_from_place(city, network_type="bike")

# Precompute edge lengths (meters)
G = ox.add_edge_travel_times(G)

def sample_route_data(route_id, G, start_time):
    # 2. Pick two random nodes for origin & destination
    origin = random.choice(list(G.nodes))
    destination = random.choice(list(G.nodes))
    # 3. Compute shortest path (by length)
    path = nx.shortest_path(G, origin, destination, weight="length")
    
    # 4. Assign a random speed (e.g. between 4 km/h and 6 km/h for walking)
    speed_kmh = random.uniform(4, 6)
    speed_m_per_s = speed_kmh * 1000 / 3600
    
    # 5. Build the spatiotemporal points
    records = []
    current_time = start_time
    for seq, u in enumerate(path, start=1):
        # get node coords
        lat = G.nodes[u]['y']
        lon = G.nodes[u]['x']
        records.append({
            "route_id": route_id,
            "sequence": seq,
            "latitude": lat,
            "longitude": lon,
            "timestamp": current_time.isoformat()
        })
        # advance time by the travel time along the edge to next node
        if seq < len(path):
            v = path[seq]  # next node
            edge_data = G.get_edge_data(u, v)[0]
            length_m = edge_data['length']
            travel_secs = length_m / speed_m_per_s
            current_time += timedelta(seconds=travel_secs)
    return records

# 6. Sample N routes with random start times over the past week
num_routes = 10
now = datetime.utcnow()
all_records = []
for rid in range(1, num_routes + 1):
    # random start within past 7 days
    start_offset = timedelta(days=random.uniform(0, 7),
                             hours=random.uniform(0, 24),
                             minutes=random.uniform(0, 60))
    start_time = now - start_offset
    all_records.extend(sample_route_data(rid, G, start_time))

# 7. Dump to CSV
df = pd.DataFrame(all_records)
df.to_csv("spatiotemporal_routes.csv", index=False)
print("Written", len(df), "points across", num_routes, "routes to spatiotemporal_routes.csv")

