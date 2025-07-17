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


# AKS Cluster
resource "azurerm_kubernetes_cluster" "main" {
  name                = var.cluster_name
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  dns_prefix          = var.cluster_name
  kubernetes_version  = "1.32"

  default_node_pool {
    name       = "benchmark"
    node_count = var.aks_node_count
    vm_size    = var.vm_size
  }

  identity {
    type = "SystemAssigned"
  }
}
