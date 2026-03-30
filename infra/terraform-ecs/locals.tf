locals {
  project_name = var.project_name

  # Use existing VPC or create new one
  vpc_id          = var.existing_vpc_id != "" ? var.existing_vpc_id : module.vpc.vpc_id
  public_subnets  = length(var.existing_public_subnet_ids) > 0 ? var.existing_public_subnet_ids : module.vpc.public_subnets
  private_subnets = length(var.existing_private_subnet_ids) > 0 ? var.existing_private_subnet_ids : module.vpc.private_subnets

  alb_sg       = var.existing_alb_sg_id != "" ? var.existing_alb_sg_id : aws_security_group.alb[0].id

  # ✅ FIXED
  ecs_tasks_sg = aws_security_group.webapp_tasks.id
}
