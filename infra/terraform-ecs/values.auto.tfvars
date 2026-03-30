aws_region   = "us-east-2"
project_name = "kafka-enterprise-orders"

# =========================
# GHCR (optional for other services)
# =========================
ghcr_username = "jumptotechschooldevops"

# =========================
# CONTAINER IMAGES (FINAL)
# =========================

# ✅ FRONTEND (ECR)
container_image_frontend = "021399177326.dkr.ecr.us-east-2.amazonaws.com/kafka-frontend:latest"

# ✅ BACKEND (ECR — FIXED AMD64 IMAGE)
container_image_backend = "021399177326.dkr.ecr.us-east-2.amazonaws.com/kafka-backend:v101"

# ⚠️ OTHER SERVICES (can migrate later)
container_image_producer  = "ghcr.io/jumptotechschooldevops/kafka-enterprise-orders-producer:v2"
container_image_fraud     = "ghcr.io/jumptotechschooldevops/kafka-enterprise-orders-fraud-service:v2"
container_image_payment   = "ghcr.io/jumptotechschooldevops/kafka-enterprise-orders-payment-service:v2"
container_image_analytics = "ghcr.io/jumptotechschooldevops/kafka-enterprise-orders-analytics-service:v2"

# =========================
# NETWORK (OPTIONAL — leave empty if using new VPC)
# =========================
# existing_vpc_id             = ""
# existing_public_subnet_ids  = []
# existing_private_subnet_ids = []
# existing_alb_sg_id          = ""
# existing_ecs_tasks_sg_id    = ""
# existing_rds_sg_id          = ""

# =========================
# CONFLUENT CLOUD
# =========================
confluent_bootstrap_servers = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"

# =========================
# COUCHBASE (FINAL)
# =========================
couchbase_host   = "cb.2s2wqp2fpzi0hanx.cloud.couchbase.com"
couchbase_bucket = "order_analytics"
