#!/bin/bash
set -e

echo "Starting VM setup at $(date)"

apt-get update
apt-get upgrade -y

mkdir /mnt/schemas
chown azureuser /mnt/schemas

mkdir /mnt/datasets
chown azureuser /mnt/datasets

# Install Determinate Nix
echo "Installing Determinate Nix..."
curl -fsSL https://install.determinate.systems/nix | sh -s -- install --determinate --no-confirm

echo "VM setup completed successfully!"
