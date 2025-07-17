output "cluster_endpoint" {
  description = "AKS cluster endpoint"
  value       = azurerm_kubernetes_cluster.main.kube_config.0.host
  sensitive   = true
}

output "cluster_name" {
  description = "AKS cluster name"
  value       = azurerm_kubernetes_cluster.main.name
}

output "location" {
  description = "Azure location"
  value       = var.azure_location
}

output "kubectl_config_command" {
  description = "Command to configure kubectl"
  value       = "az aks get-credentials --resource-group ${azurerm_resource_group.main.name} --name ${azurerm_kubernetes_cluster.main.name}"
}

# Load generator VM outputs
output "load_generator_public_ip" {
  description = "Public IP address of the load generator VM"
  value       = azurerm_public_ip.load_generator.ip_address
}

output "load_generator_ssh_connection" {
  description = "SSH connection string for load generator VM"
  value       = "ssh ${var.load_generator_admin_username}@${azurerm_public_ip.load_generator.ip_address}"
}
