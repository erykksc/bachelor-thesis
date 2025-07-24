output "cluster_endpoint" {
  description = "AKS cluster endpoint"
  value       = azurerm_kubernetes_cluster.main.kube_config.0.host
  sensitive   = true
}

output "cluster_name" {
  description = "AKS cluster name"
  value       = azurerm_kubernetes_cluster.main.name
}

output "kubectl_config_command" {
  description = "Command to configure kubectl"
  value       = "az aks get-credentials --resource-group ${data.terraform_remote_state.shared.outputs.resource_group_name} --name ${azurerm_kubernetes_cluster.main.name}"
}