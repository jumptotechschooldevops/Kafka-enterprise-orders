variable "aws_region" {
  default = "us-east-2"
}

variable "cluster_name" {
  default = "keo-eks"
}

variable "vpc_cidr" {
  default = "10.20.0.0/16"
}

variable "public_subnets" {
  default = ["10.20.1.0/24", "10.20.2.0/24"]
}

variable "private_subnets" {
  default = ["10.20.3.0/24", "10.20.4.0/24"]
}

# GHCR
variable "ghcr_username" {
  default = "aisalkyn85"
}

variable "ghcr_pat" {
  sensitive = true
}

# Domain
variable "app_domain" {
  default = "orders.jumptotech.net"
}

variable "domain_zone" {
  default = "jumptotech.net"
}

variable "certificate_arn" {
  default = "arn:aws:acm:us-east-2:343218219153:certificate/bfd58b06-35f1-4fcf-bdd1-76e4e5f72dd2"
}

# CIDRs allowed to reach the EKS API endpoint publicly.
# Add your office IP and GitHub Actions runner CIDRs here.
# Never use ["0.0.0.0/0"] in production.
variable "allowed_public_cidrs" {
  description = "CIDRs that may access the EKS API server endpoint"
  type        = list(string)
  default     = []   # override in secrets.tfvars: allowed_public_cidrs = ["YOUR_IP/32"]
}

# Couchbase
variable "couchbase_host" {
  default = "cb.2s2wqp2fpzi0hanx.cloud.couchbase.com"
}

variable "couchbase_bucket" {
  default = "order_analytics"
}

variable "couchbase_username" {
  default = "appuser"
}

variable "couchbase_password" {
  sensitive = true
}