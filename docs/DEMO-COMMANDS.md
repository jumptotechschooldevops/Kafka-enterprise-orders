# Quick Demo Commands

## Local Testing (Docker)

Copy `.env.example` to `.env` and fill in local-only values. Never commit `.env`.

```bash
docker compose up -d --build

# Check logs
docker compose logs -f producer
docker compose logs -f analytics-service

# Open browser
# Frontend:  http://localhost:3000
# Kafdrop:   http://localhost:9000
# Grafana:   http://localhost:3001
# Backend:   http://localhost:8000/healthz

# Direct API (requires X-API-Key from your local .env / API_KEYS)
# curl -H "X-API-Key: YOUR_LOCAL_API_KEY" http://localhost:8000/api/analytics

# Through nginx (API key is injected server-side; no header needed)
# curl http://localhost:3000/api/analytics

docker compose down
```

Local Couchbase is initialized by `couchbase-init` (bucket `order_analytics`).
Console: http://localhost:8091 — use the local username/password from `.env`.

---

## EKS Deploy

Secrets must come from a gitignored `secrets.tfvars` (see `infra/terraform-eks/secrets.tfvars.example`). Never pass real credentials on the command line or commit them to git.

```bash
cd infra/terraform-eks
cp secrets.tfvars.example secrets.tfvars
# Edit secrets.tfvars locally — do not commit it
terraform init
terraform apply -var-file="secrets.tfvars"
```

## EKS Verify
```bash
aws eks update-kubeconfig --name keo-eks --region us-east-2
kubectl get nodes
kubectl get pods
kubectl get pods -n argocd
kubectl get ingress
```

## EKS Cleanup
```bash
cd infra/terraform-eks
terraform destroy -var-file="secrets.tfvars"
```

---

## ECS Deploy

```bash
cd infra/terraform-ecs
cp secrets.tfvars.example secrets.tfvars
# Edit secrets.tfvars locally — do not commit it
terraform init
terraform apply -var-file="values.auto.tfvars" -var-file="secrets.tfvars"
```

## ECS Verify (AWS Console)
- ECS → Clusters → kafka-enterprise-orders-cluster
- Check running tasks count
- CloudWatch → Log groups → /ecs/kafka-enterprise-orders-*

## ECS Cleanup
```bash
cd infra/terraform-ecs
terraform destroy -var-file="values.auto.tfvars" -var-file="secrets.tfvars"
```

---

## Test URLs

### EKS (after deploy)
```bash
curl https://orders.jumptotech.net/healthz
# /api/analytics requires authentication; do not embed keys in docs
```

### ECS (get ALB URL from terraform output)
```bash
terraform output
curl http://ALB_URL/healthz
```

---

## Couchbase Query (Capella UI)
```sql
SELECT * FROM order_analytics LIMIT 10;
SELECT COUNT(*) FROM order_analytics;
```

## Argo CD Access
```bash
# Get the initial admin password from the cluster secret (not from git)
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d
echo
kubectl get svc -n argocd argocd-server
```

---

## Configuration reference (placeholders only)

Set real values in gitignored files (`.env`, `secrets.tfvars`, Helm secrets). Never commit them.

| Key | Where to set | Example placeholder |
|-----|----------------|---------------------|
| GHCR username | Helm / Terraform var | `your-ghcr-username` |
| Couchbase host | `.env` / `secrets.tfvars` | `cb.xxxx.cloud.couchbase.com` |
| Couchbase user | `.env` / `secrets.tfvars` | `your-couchbase-user` |
| Couchbase password | `.env` / `secrets.tfvars` | `your-couchbase-password` |
| Confluent bootstrap | `.env` / `secrets.tfvars` | `pkc-xxxx.region.aws.confluent.cloud:9092` |
| Confluent API key | `.env` / `secrets.tfvars` | `your-confluent-api-key` |
| Confluent API secret | `.env` / `secrets.tfvars` | `your-confluent-api-secret` |
| Domain | Terraform var | `orders.example.com` |
| ACM certificate ARN | `secrets.tfvars` | `arn:aws:acm:region:account:certificate/id` |
| Backend API key (local) | `.env` `API_KEYS` / `BACKEND_API_KEY` | `dev-key-change-me` |
