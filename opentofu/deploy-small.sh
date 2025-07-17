#!/bin/sh

set -e

load_generator_ssh_public_key="$(cat ~/.ssh/id_rsa.pub)" 
cluster_name="my-benchmark-cluster-small" 
aks_node_count=2 
azure_location="West Europe" 

echo "Planning the infrastructure deployment using tofu..."

# Plan the infrastructure deployment
tofu plan \
   -var "load_generator_ssh_public_key=$load_generator_ssh_public_key" \
   -var "cluster_name=$cluster_name" \
   -var "aks_node_count=$aks_node_count" \
   -var "azure_location=$azure_location" \
   -out="small-deploy.tofuplan"

# Wait for user to confirm whether he wants to continue
read -p "Do you want to deply the created plan? (y to confirm): " answer
if [[ "$answer" == "y" ]]; then
    echo "Applying the created plan..."
    # Place your script logic here
else
    echo "Exiting without applying the infrastructure."
    exit 0
fi

# Deploy the infrastrucutre
tofu apply "small-deploy.tofuplan"
