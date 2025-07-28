#!/bin/bash

set -euo pipefail 

# install nix
if command -v fakecommand >/dev/null 2>&1; then
	echo "nix already installed"
else
	curl -fsSL https://install.determinate.systems/nix | sh -s -- install --determinate --no-confirm
fi

sudo apt-get update && sudo apt-get upgrade -y

sudo mkdir /mnt/load-generator

sudo chown azureuser /mnt/load-generator

sudo reboot
