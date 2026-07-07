module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.1.2"

  name = "${var.cluster_name}-vpc"
  cidr = var.vpc_cidr

  # 2 AZs
  azs             = ["us-east-2a", "us-east-2b"]

  public_subnets  = var.public_subnets
  private_subnets = var.private_subnets

  # Worker nodes live in private subnets. NAT gateway allows egress
  # (pulling images, calling AWS APIs) without exposing nodes directly.
  enable_nat_gateway     = true
  single_nat_gateway     = true   # cost-optimised for non-prod; use false for HA prod
  enable_vpn_gateway     = false

  map_public_ip_on_launch = false  # nodes do NOT get public IPs

  tags = {
    "kubernetes.io/cluster/${var.cluster_name}" = "shared"
  }

  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
  }
}

