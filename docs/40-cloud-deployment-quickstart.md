# Cloud Deployment Quickstart

This guide provides a faster path for AWS/GCP/Azure deployments using:
- script-driven Helm deploys on existing clusters
- Terraform full provisioning (network + cluster + ingress + secret manager + Helm)

## What This Adds

- Cloud Helm presets:
  - `deployment/helm/presets/aws-eks.yaml`
  - `deployment/helm/presets/gcp-gke.yaml`
  - `deployment/helm/presets/azure-aks.yaml`
- Deploy wrapper:
  - `tools/deploy/idr_deploy.sh`
- Secret bootstrap helper:
  - `tools/deploy/bootstrap_k8s_secrets.sh`
- OIDC override generator:
  - `tools/deploy/bootstrap_oidc_values.sh`
- Terraform full modules:
  - `deployment/terraform/aws-eks/main.tf`
  - `deployment/terraform/gcp-gke/main.tf`
  - `deployment/terraform/azure-aks/main.tf`

## Prerequisites

- `helm`
- `kubectl`
- `terraform`
- cloud provider credentials configured (AWS/GCP/Azure)
- container images available in your target registry

## Step 1: Create/Reconcile Kubernetes Secrets

```bash
bash tools/deploy/bootstrap_k8s_secrets.sh \
  --namespace idr \
  --secret-name idr-enterprise-secrets
```

## Step 2: (Optional) Generate OIDC Overrides

```bash
bash tools/deploy/bootstrap_oidc_values.sh \
  --issuer "https://your-idp.example.com/realms/idr" \
  --audience "account" \
  --output /tmp/idr-oidc-values.yaml
```

## Step 3: Plan Deployment

```bash
idr deploy \
  --provider aws \
  --mode plan \
  --use-existing-secret idr-enterprise-secrets \
  --values /tmp/idr-oidc-values.yaml
```

Switch `--provider` to `gcp` or `azure` as needed.

## Step 4: Apply Deployment

```bash
idr deploy \
  --provider aws \
  --mode apply \
  --use-existing-secret idr-enterprise-secrets \
  --values /tmp/idr-oidc-values.yaml
```

## Step 5: Validate with Cluster Doctor

```bash
export IDR_TOKEN="<bearer-token>"
idr doctor --target cluster \
  --api-url "https://idr.example.com/api/health" \
  --metrics-url "https://idr.example.com/metrics" \
  --whoami-url "https://idr.example.com/api/auth/whoami" \
  --token-env IDR_TOKEN \
  --json
```

## CI/CD Template Usage

Use these commands in your deployment pipeline stages:

1. `bootstrap_k8s_secrets.sh` to ensure secret state.
2. `idr_deploy.sh --mode plan` as pre-merge gate.
3. `idr_deploy.sh --mode apply` for staged rollout.
4. `idr doctor --target cluster` as post-deploy verification gate.

Reference workflow template:
- `tools/ci/templates/github-actions-cloud-deploy.yml`

## Terraform Option

If you prefer IaC-driven full provisioning, use:
- `deployment/terraform/README.md`

### AWS (EKS) Full Provision

```bash
cd deployment/terraform/aws-eks
terraform init
terraform apply \
  -var region=us-east-1 \
  -var ingress_hostname=idr.example.com
```

Optional Route53 DNS automation:

```bash
terraform apply \
  -var region=us-east-1 \
  -var ingress_hostname=idr.example.com \
  -var create_dns_record=true \
  -var route53_zone_id=Z1234567890ABC
```

### GCP (GKE) Full Provision

```bash
cd deployment/terraform/gcp-gke
terraform init
terraform apply \
  -var project_id=my-gcp-project \
  -var region=us-central1 \
  -var zone=us-central1-a \
  -var ingress_hostname=idr.example.com
```

Optional Cloud DNS automation:

```bash
terraform apply \
  -var project_id=my-gcp-project \
  -var region=us-central1 \
  -var zone=us-central1-a \
  -var ingress_hostname=idr.example.com \
  -var create_dns_record=true \
  -var dns_managed_zone=example-com
```

### Azure (AKS) Full Provision

```bash
cd deployment/terraform/azure-aks
terraform init
terraform apply \
  -var resource_group_name=idr-rg \
  -var location=eastus \
  -var ingress_hostname=idr.example.com
```

Optional Azure DNS automation:

```bash
terraform apply \
  -var resource_group_name=idr-rg \
  -var location=eastus \
  -var ingress_hostname=idr.example.com \
  -var create_dns_record=true \
  -var dns_zone_name=example.com \
  -var dns_zone_resource_group=shared-dns-rg
```

### Terraform Static Validation (All Providers)

```bash
terraform fmt -recursive deployment/terraform

cd deployment/terraform/aws-eks && terraform init -backend=false && terraform validate
cd ../gcp-gke && terraform init -backend=false && terraform validate
cd ../azure-aks && terraform init -backend=false && terraform validate
```
