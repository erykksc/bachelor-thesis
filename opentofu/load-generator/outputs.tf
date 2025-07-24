output "public_ip" {
  description = "Public IP address of the load generator VM"
  value       = azurerm_public_ip.load_generator.ip_address
}

output "ssh_connection" {
  description = "SSH connection string for load generator VM"
  value       = "ssh ${var.admin_username}@${azurerm_public_ip.load_generator.ip_address}"
}

output "vm_name" {
  description = "Name of the load generator VM"
  value       = azurerm_linux_virtual_machine.load_generator.name
}