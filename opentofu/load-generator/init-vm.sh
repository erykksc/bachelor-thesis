#!/bin/bash

set -euo pipefail 

# install nix
if command -v nix >/dev/null 2>&1; then
	echo "nix already installed"
else
	curl -fsSL https://install.determinate.systems/nix | sh -s -- install --determinate --no-confirm
fi

sudo apt-get update && sudo apt-get upgrade -y

sudo mkdir /mnt/ba
sudo chown azureuser /mnt/ba

sudo reboot
