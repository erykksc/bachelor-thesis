# AKS specific variables
variable "kubernetes_version" {
  description = "Kubernetes version for AKS cluster"
  type        = string
  default     = "1.32"
}

variable "node_count" {
  description = "Number of AKS worker nodes"
  type        = number
  default     = 3
}

variable "vm_size" {
  description = "Azure VM size for AKS nodes"
  type        = string
  default     = "Standard_D2ads_v6"
}
