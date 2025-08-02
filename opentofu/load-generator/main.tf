terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Read shared infrastructure state
data "terraform_remote_state" "shared" {
  backend = "local"
  config = {
    path = "../shared/terraform.tfstate"
  }
}

# Public IP for load generator VM
# Required to access the VM from outside Azure for SSH management
# and to allow the VM to make outbound connections to the internet
resource "azurerm_public_ip" "load_generator" {
  name                = "${data.terraform_remote_state.shared.outputs.project_name}-load-generator-ip"
  location            = data.terraform_remote_state.shared.outputs.resource_group_location
  resource_group_name = data.terraform_remote_state.shared.outputs.resource_group_name
  allocation_method   = "Static"   # Static IP ensures consistent access
  sku                 = "Standard" # Standard SKU required for availability zones
}

# Network Security Group for load generator
# Controls inbound/outbound traffic to the VM
# Essential for securing SSH access and allowing benchmark traffic
resource "azurerm_network_security_group" "load_generator" {
  name                = "${data.terraform_remote_state.shared.outputs.project_name}-load-generator-nsg"
  location            = data.terraform_remote_state.shared.outputs.resource_group_location
  resource_group_name = data.terraform_remote_state.shared.outputs.resource_group_name

  # SSH access rule - allows remote management of the load generator
  # Port 22 is the standard SSH port for Linux administration
  security_rule {
    name                       = "SSH"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = "*" # Consider restricting to specific IPs
    destination_address_prefix = "*"
  }

  # Outbound rules are implicitly allowed by default
  # VM needs outbound access to reach AKS cluster services and download tools
}

# Network Interface for load generator VM
# Connects the VM to Azure's network infrastructure
# Required for any VM to communicate with other resources
resource "azurerm_network_interface" "load_generator" {
  name                = "${data.terraform_remote_state.shared.outputs.project_name}-load-generator-nic"
  location            = data.terraform_remote_state.shared.outputs.resource_group_location
  resource_group_name = data.terraform_remote_state.shared.outputs.resource_group_name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = data.terraform_remote_state.shared.outputs.subnet_id
    private_ip_address_allocation = "Dynamic" # Azure assigns IP automatically
    public_ip_address_id          = azurerm_public_ip.load_generator.id
  }
}

# Associate Network Security Group with Network Interface
# Links the security rules to the VM's network interface
# Without this, the NSG rules won't apply to the VM
resource "azurerm_network_interface_security_group_association" "load_generator" {
  network_interface_id      = azurerm_network_interface.load_generator.id
  network_security_group_id = azurerm_network_security_group.load_generator.id
}

# Linux VM for load generation and benchmarking
# Separate VM ensures load generation doesn't interfere with AKS cluster performance
# Provides isolated environment for running benchmark tools against the cluster
resource "azurerm_linux_virtual_machine" "load_generator" {
  name                = "benchmark-load-generator"
  location            = data.terraform_remote_state.shared.outputs.resource_group_location
  resource_group_name = data.terraform_remote_state.shared.outputs.resource_group_name
  size                = var.vm_size
  admin_username      = var.admin_username

  # SSH key authentication is more secure than passwords
  disable_password_authentication = true

  network_interface_ids = [
    azurerm_network_interface.load_generator.id,
  ]

  # SSH public key for secure access
  # Private key should be kept secure on the client machine
  admin_ssh_key {
    username   = var.admin_username
    public_key = var.ssh_public_key
  }

  # OS disk configuration
  # Premium_LRS provides better performance for I/O operations
  # ReadWrite caching improves disk performance
  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }

  # Run setup script on first boot
  custom_data = base64encode(file("${path.module}/init-vm.sh"))
}
