output "resource_group_name" {
  description = "Name of the resource group"
  value       = azurerm_resource_group.main.name
}

output "resource_group_location" {
  description = "Location of the resource group"
  value       = azurerm_resource_group.main.location
}

output "vnet_name" {
  description = "Name of the virtual network"
  value       = azurerm_virtual_network.main.name
}

output "subnet_id" {
  description = "ID of the main subnet"
  value       = azurerm_subnet.main.id
}

output "project_name" {
  description = "Base project name"
  value       = var.project_name
}