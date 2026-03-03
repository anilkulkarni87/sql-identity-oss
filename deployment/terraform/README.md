# Terraform Full Cloud Provisioning

These provider modules support full-stack provisioning for IDR enterprise:

- `deployment/terraform/aws-eks`
- `deployment/terraform/gcp-gke`
- `deployment/terraform/azure-aks`

Each module can:
- create network foundations (VPC/VNet + subnets)
- create managed Kubernetes cluster (EKS/GKE/AKS)
- provision secret manager integration (AWS Secrets Manager / GCP Secret Manager / Azure Key Vault)
- create Kubernetes runtime secret for the Helm chart (or reuse existing secret)
- install ingress-nginx and publish an app Ingress
- optionally create DNS records in Route53 / Cloud DNS / Azure DNS
- deploy `deployment/helm/idr-enterprise`

Each module also supports attachment mode:
- `create_network=false` to use existing networking
- `create_cluster=false` to target existing managed clusters

## AWS (EKS) Quick Start

```bash
cd deployment/terraform/aws-eks
terraform init
terraform apply -var region=us-east-1 -var ingress_hostname=idr.example.com
```

## GCP (GKE) Quick Start

```bash
cd deployment/terraform/gcp-gke
terraform init
terraform apply \
  -var project_id=my-gcp-project \
  -var region=us-central1 \
  -var zone=us-central1-a \
  -var ingress_hostname=idr.example.com
```

## Azure (AKS) Quick Start

```bash
cd deployment/terraform/azure-aks
terraform init
terraform apply \
  -var resource_group_name=idr-rg \
  -var location=eastus \
  -var ingress_hostname=idr.example.com
```

## External OIDC Pattern

All modules support external IdP wiring with:
- `external_oidc_enabled=true`
- `external_oidc_issuer=<issuer-url>`
- `external_oidc_jwks_url=<jwks-url>`
- `external_oidc_audience=<audience>`

## Secret Manager Integration

Each module can provision and populate runtime secrets:
- AWS: `aws_secretsmanager_secret`
- GCP: `google_secret_manager_secret` (+ versions)
- Azure: `azurerm_key_vault` + `azurerm_key_vault_secret`

Set `create_secret_manager=false` to skip cloud secret manager creation and let Terraform generate a Kubernetes secret directly (or set `use_existing_secret_name` to reuse one).

## Validation Commands

Run these on a machine with Terraform installed:

```bash
terraform fmt -recursive deployment/terraform

cd deployment/terraform/aws-eks && terraform init -backend=false && terraform validate
cd ../gcp-gke && terraform init -backend=false && terraform validate
cd ../azure-aks && terraform init -backend=false && terraform validate
```

## Post-Deploy Validation

```bash
export IDR_TOKEN="<bearer-token>"
idr doctor --target cluster \
  --api-url "https://idr.example.com/api/health" \
  --metrics-url "https://idr.example.com/metrics" \
  --whoami-url "https://idr.example.com/api/auth/whoami" \
  --token-env IDR_TOKEN \
  --json
```

## Optional DNS Automation

Enable DNS automation per provider:
- AWS: `create_dns_record=true` + `route53_zone_id=<hosted-zone-id>` (creates Route53 CNAME)
- GCP: `create_dns_record=true` + `dns_managed_zone=<managed-zone-name>` (creates Cloud DNS A record)
- Azure: `create_dns_record=true` + `dns_zone_name=<zone>` (creates Azure DNS A record)

If ingress external address is not ready during the first apply, rerun `terraform apply` once ingress has an external address, or set `dns_target_override`.
