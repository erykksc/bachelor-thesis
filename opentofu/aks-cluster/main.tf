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

# AKS Cluster
resource "azurerm_kubernetes_cluster" "main" {
  name                = "${data.terraform_remote_state.shared.outputs.project_name}-aks-cluster"
  location            = data.terraform_remote_state.shared.outputs.resource_group_location
  resource_group_name = data.terraform_remote_state.shared.outputs.resource_group_name
  dns_prefix          = "${data.terraform_remote_state.shared.outputs.project_name}-aks"
  kubernetes_version  = var.kubernetes_version

  default_node_pool {
    name           = "benchmark"
    node_count     = var.node_count
    vm_size        = var.vm_size
    vnet_subnet_id = data.terraform_remote_state.shared.outputs.subnet_id
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