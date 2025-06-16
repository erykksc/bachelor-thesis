variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "cluster_name" {
  description = "EKS cluster name"
  type        = string
  default     = "mobilitydb-benchmark"
}

variable "node_count" {
  description = "Number of EKS worker nodes"
  type        = number
  default     = 3
}

variable "instance_type" {
  description = "EC2 instance type for EKS nodes"
  type        = string
  default     = "m5.large"
}

variable "benchmark_configs" {
  description = "List of benchmark configurations to deploy"
  type = list(object({
    name            = string
    worker_replicas = number
  }))
  default = [
    {
      name            = "small-cluster"
      worker_replicas = 3
    },
    {
      name            = "large-cluster"
      worker_replicas = 20
    }
  ]
}
