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

# Resource Group
resource "azurerm_resource_group" "main" {
  name     = "${var.cluster_name}-rg"
  location = var.azure_location
}


# Virtual Network for AKS and load generator
resource "azurerm_virtual_network" "main" {
  name                = "${var.cluster_name}-vnet"
  address_space       = ["10.0.0.0/16"]
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
}

# Subnet for AKS cluster
resource "azurerm_subnet" "aks" {
  name                 = "aks-subnet"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.0.1.0/24"]
}

# Subnet for load generator
resource "azurerm_subnet" "load_generator" {
  name                 = "load-generator-subnet"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.0.2.0/24"]
}

# AKS Cluster
resource "azurerm_kubernetes_cluster" "main" {
  name                = var.cluster_name
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  dns_prefix          = var.cluster_name
  kubernetes_version  = "1.32"

  default_node_pool {
    name           = "benchmark"
    node_count     = var.aks_node_count
    vm_size        = var.vm_size
    vnet_subnet_id = azurerm_subnet.aks.id
  }

  network_profile {
    network_plugin = "azure"
    service_cidr   = "192.168.0.0/16"
    dns_service_ip = "192.168.0.10"
  }

  identity {
    type = "SystemAssigned"
  }
}

# Public IP for load generator VM
# Required to access the VM from outside Azure for SSH management
# and to allow the VM to make outbound connections to the internet
resource "azurerm_public_ip" "load_generator" {
  name                = "${var.cluster_name}-load-generator-ip"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  allocation_method   = "Static"   # Static IP ensures consistent access
  sku                 = "Standard" # Standard SKU required for availability zones
}

# Network Security Group for load generator
# Controls inbound/outbound traffic to the VM
# Essential for securing SSH access and allowing benchmark traffic
resource "azurerm_network_security_group" "load_generator" {
  name                = "${var.cluster_name}-load-generator-nsg"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

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
  name                = "${var.cluster_name}-load-generator-nic"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.load_generator.id
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
  name                = "${var.cluster_name}-load-generator"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  size                = var.load_generator_vm_size
  admin_username      = var.load_generator_admin_username

  # SSH key authentication is more secure than passwords
  disable_password_authentication = true

  network_interface_ids = [
    azurerm_network_interface.load_generator.id,
  ]

  # SSH public key for secure access
  # Private key should be kept secure on the client machine
  admin_ssh_key {
    username   = var.load_generator_admin_username
    public_key = var.load_generator_ssh_public_key
  }

  # OS disk configuration
  # Premium_LRS provides better performance for I/O operations
  # ReadWrite caching improves disk performance
  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
  }

  # Ubuntu 22.04 LTS - stable, long-term support Linux distribution
  # Widely used for development and has good tooling support
  # Gen2 provides better performance and security features
  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }
}
