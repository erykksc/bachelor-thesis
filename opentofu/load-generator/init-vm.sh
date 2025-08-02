#!/bin/bash

set -euo pipefail 

# MOUNT THE DRIVE
DISK="/dev/nvme1n1"
MOUNT_POINT="/mnt/data"

timeout=30
while [ ! -b "$DISK" ] && [ $timeout -gt 0 ]; do
  echo "Waiting for $DISK to be available..."
  sleep 1
  timeout=$((timeout - 1))
done

if [ ! -b "$DISK" ]; then
  echo "Disk $DISK not found. Exiting."
  exit 1
fi

# Check if the disk already has a filesystem
if ! blkid "$DISK" > /dev/null 2>&1; then
  echo "Formatting $DISK as ext4..."
  mkfs.ext4 -F "$DISK"
else
  echo "Disk $DISK already formatted."
fi

# Create mount point if not exists
if [ ! -d "$MOUNT_POINT" ]; then
  echo "Creating mount point $MOUNT_POINT"
  mkdir -p "$MOUNT_POINT"
fi

# Backup fstab
cp /etc/fstab /etc/fstab.backup

# Add mount entry to /etc/fstab if not already present
if ! grep -qs "^$DISK" /etc/fstab; then
  echo "Adding $DISK to /etc/fstab"
  echo "$DISK $MOUNT_POINT ext4 defaults,nofail 0 2" >> /etc/fstab
else
  echo "$DISK already in /etc/fstab"
fi

echo "Mounting $DISK to $MOUNT_POINT"
mount "$MOUNT_POINT"

mountpoint -q "$MOUNT_POINT" && echo "Disk mounted successfully" || echo "Failed to mount disk"


# POST DISK MOUNT

# install nix
if command -v nix >/dev/null 2>&1; then
	echo "nix already installed"
else
	curl -fsSL https://install.determinate.systems/nix | sh -s -- install --determinate --no-confirm
fi

sudo apt-get update && sudo apt-get upgrade -y

sudo mkdir /mnt/data/ba
sudo chown azureuser /mnt/data/ba

sudo reboot
