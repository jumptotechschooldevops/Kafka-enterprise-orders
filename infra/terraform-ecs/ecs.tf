# =========================
# ECS CLUSTER
# =========================
resource "aws_ecs_cluster" "this" {
  name = "${var.project_name}-cluster"
}

# =========================
# IAM ROLE
# =========================
resource "aws_iam_role" "ecs_task_execution" {
  name = "${var.project_name}-ecs-task-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = { Service = "ecs-tasks.amazonaws.com" },
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_policy" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# =========================
# SECURITY GROUP
# =========================
resource "aws_security_group" "webapp_tasks" {
  name   = "${var.project_name}-webapp-tasks-sg"
  vpc_id = local.vpc_id

  ingress {
    from_port       = 80
    to_port         = 80
    protocol        = "tcp"
    security_groups = [local.alb_sg]
  }

  ingress {
    from_port       = 8000
    to_port         = 8000
    protocol        = "tcp"
    security_groups = [local.alb_sg]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# =========================
# LOG GROUPS
# =========================
resource "aws_cloudwatch_log_group" "frontend" {
  name              = "/ecs/${var.project_name}-frontend"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_group" "backend" {
  name              = "/ecs/${var.project_name}-backend"
  retention_in_days = 7
}

# =========================
# FRONTEND TASK (ECR)
# =========================
resource "aws_ecs_task_definition" "frontend" {
  family                   = "${var.project_name}-frontend"
  cpu                      = "256"
  memory                   = "512"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn

  container_definitions = jsonencode([{
    name      = "frontend"
    image     = var.container_image_frontend
    essential = true

    portMappings = [{
      containerPort = 80
      hostPort      = 80
    }]

    logConfiguration = {
      logDriver = "awslogs",
      options = {
        awslogs-group         = aws_cloudwatch_log_group.frontend.name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "ecs"
      }
    }
  }])
}

# =========================
# BACKEND TASK (ECR + ENV)
# =========================
resource "aws_ecs_task_definition" "backend" {
  family                   = "${var.project_name}-backend"
  cpu                      = "512"
  memory                   = "1024"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn

  container_definitions = jsonencode([{
    name      = "backend"
    image     = var.container_image_backend
    essential = true

    portMappings = [{
      containerPort = 8000
      hostPort      = 8000
    }]

    environment = [
      {
        name  = "COUCHBASE_HOST"
        value = var.couchbase_host
      },
      {
        name  = "COUCHBASE_BUCKET"
        value = var.couchbase_bucket
      },
      {
        name  = "COUCHBASE_USERNAME"
        value = var.couchbase_username
      },
      {
        name  = "COUCHBASE_PASSWORD"
        value = var.couchbase_password
      }
    ]

    logConfiguration = {
      logDriver = "awslogs",
      options = {
        awslogs-group         = aws_cloudwatch_log_group.backend.name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "ecs"
      }
    }
  }])
}

# =========================
# FRONTEND SERVICE
# =========================
resource "aws_ecs_service" "frontend" {
  name            = "${var.project_name}-frontend-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.frontend.arn
  desired_count   = 2
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = local.public_subnets
    security_groups  = [aws_security_group.webapp_tasks.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.webapp_tg.arn
    container_name   = "frontend"
    container_port   = 80
  }

  depends_on = [aws_lb_listener.http]
}

# =========================
# BACKEND SERVICE
# =========================
resource "aws_ecs_service" "backend" {
  name            = "${var.project_name}-backend-svc"
  cluster         = aws_ecs_cluster.this.id
  task_definition = aws_ecs_task_definition.backend.arn
  desired_count   = 2
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = local.public_subnets
    security_groups  = [aws_security_group.webapp_tasks.id]
    assign_public_ip = true
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.backend_tg.arn
    container_name   = "backend"
    container_port   = 8000
  }

  depends_on = [aws_lb_listener.http]
}
