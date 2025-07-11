#!/bin/bash

set -e

# Register the hostname that future workers will use to connect
# to the coordinator node.
echo "Register coordinator's hostname"
docker exec coordinator gosu postgres psql -c \
    "SELECT citus_set_coordinator_host('coordinator', 5432);"


# Check if at least one argument is provided
if [ "$#" -lt 1 ]; then
    echo "Usage: $0 node_name:port:username:password [more_nodes...]"
    exit 1
fi

echo "Create .pgpass and set permissions"
docker exec coordinator bash -c \
    "touch /var/lib/postgresql/.pgpass &&\
    chmod 600 /var/lib/postgresql/.pgpass &&\
    chown postgres:postgres /var/lib/postgresql/.pgpass"

# Loop over all arguments
for arg in "$@"; do
    IFS=':' read -r node_name port username password <<< "$arg:postgres:password"

    # Validate each argument
    if [ -z "$node_name" ] || [ -z "$port" ] || [ -z "$username" ] || [ -z "$password" ]; then
        echo "Invalid argument format: $arg"
        echo "Expected format: node_name:port:username:password"
        continue
    fi

    # Print parsed data
    echo "Adding worker node: $node_name, port: $port, username: $username, password:$password"

    # Example command using the parsed data
    echo "Adding password to /var/lib/postgresql/.pgpass"
    docker exec coordinator bash -c \
        "echo \"$node_name:$port:*:$username:$password\" >> /var/lib/postgresql/.pgpass"
    
    echo "Adding node to coordinator"
    docker exec coordinator gosu postgres psql -c \
        "SELECT * from citus_add_node('$node_name', $port);"
    echo ""
done

echo "Finished adding worker nodes, getting active worker nodes"
docker exec coordinator gosu postgres psql -c \
    "SELECT * FROM citus_get_active_worker_nodes();"
