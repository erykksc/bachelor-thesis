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
