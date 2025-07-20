variable "azure_location" {
  description = "Azure location"
  type        = string
  default     = "West Europe"
}

variable "cluster_name" {
  description = "AKS cluster name"
  type        = string
  default     = "benchmark-aks-cluster"
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

# by default 4 cores, 16GB of RAM, 150GB of storage (important to fit the dataset)
variable "load_generator_vm_size" {
  description = "Azure VM size for load generator"
  type        = string
  default     = "Standard_D4ds_v4"
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
