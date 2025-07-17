variable "azure_location" {
  description = "Azure location"
  type        = string
  default     = "France Central"
}

variable "cluster_name" {
  description = "AKS cluster name"
  type        = string
  default     = "thesis-aks-cluster"
}

variable "aks_node_count" {
  description = "Number of AKS worker nodes"
  type        = number
  default     = 3
}

variable "vm_size" {
  description = "Azure VM size for AKS nodes"
  type        = string
  default     = "standard_d2ds_v4"
}

variable "load_generator_vm_size" {
  description = "Azure VM size for load generator"
  type        = string
  default     = "Standard_D2s_v3"
}

variable "load_generator_admin_username" {
  description = "Admin username for load generator VM"
  type        = string
  default     = "azureuser"
}

variable "load_generator_ssh_public_key" {
  description = "SSH public key for load generator VM access"
  type        = string
}
