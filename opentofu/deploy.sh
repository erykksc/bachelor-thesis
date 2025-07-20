#!/bin/sh

set -e

# Validate input
if [ $# -ne 1 ]; then
    echo "Usage: $0 {small|medium|large|xl}"
    exit 1
fi

config="$1"
load_generator_ssh_public_key="$(cat ~/.ssh/id_rsa.pub)"

# Select config
case "$config" in
    small)
        aks_node_count=2
        ;;
    medium)
        aks_node_count=3
        ;;
    large)
        aks_node_count=4
        ;;
    xl)
        aks_node_count=5
        ;;
    *)
        echo "Invalid configuration: $config"
        echo "Valid options: small, medium, large, xl"
        exit 1
        ;;
esac

echo "Selected configuration: $config"
echo "Planning the infrastructure deployment using tofu..."

# Plan the deployment
plan_file="${config}-deploy.tofuplan"

tofu plan \
   -var "load_generator_ssh_public_key=$load_generator_ssh_public_key" \
   -var "aks_node_count=$aks_node_count" \
   -out="$plan_file"

# Prompt to apply
read -p "Do you want to deploy the created plan? (y to confirm): " answer
if [ "$answer" != "y" ]; then
    echo "Exiting without applying the infrastructure."
    exit 0
fi

echo "Applying the created plan..."
tofu apply "$plan_file"
