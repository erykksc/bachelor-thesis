# Load generator specific variables
variable "vm_size" {
  description = "Azure VM size for load generator"
  type        = string
  default     = "Standard_D4alds_v6"
}

variable "admin_username" {
  description = "Admin username for load generator VM"
  type        = string
  default     = "azureuser"
}

variable "ssh_public_key" {
  description = "SSH public key for load generator VM access"
  type        = string
}

