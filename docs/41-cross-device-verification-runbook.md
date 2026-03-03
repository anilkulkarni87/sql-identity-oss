# Cross-Device Verification Runbook

Use this on a fresh machine and share results back.

## Quick path: single wrapper script

```bash
bash tools/ci/run_cross_device_verification.sh
```

With optional provider DNS plan check:

```bash
bash tools/ci/run_cross_device_verification.sh \
  --provider aws \
  --run-terraform-plan
```

With optional docker runtime checks:

```bash
bash tools/ci/run_cross_device_verification.sh --run-docker-checks
```

With both:

```bash
bash tools/ci/run_cross_device_verification.sh \
  --provider gcp \
  --run-terraform-plan \
  --run-docker-checks
```

If you prefer manual step-by-step execution, use the sections below.

## 1) Start a full log capture

```bash
export LOG_FILE="/tmp/idr_cross_device_verify_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1
echo "LOG_FILE=$LOG_FILE"
```

## 2) Clone repo and verify prerequisites

```bash
git clone https://github.com/anilkulkarni87/sql-identity-resolution.git
cd sql-identity-resolution

python3 --version
node --version
npm --version
docker --version
docker compose version
helm version
terraform version
```

## 3) Python environment + backend checks

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements/ci.lock
python -m pip install -r requirements/release.lock
python -m pip install -e .

PYTHONPATH=. ruff check idr_core idr_api tests tools
PYTHONPATH=. pytest \
  tests/test_api.py \
  tests/test_auth.py \
  tests/test_cli_smoke.py \
  tests/test_doctor.py \
  tests/test_mcp_contract.py \
  tests/test_mcp_secrets.py \
  tests/test_mcp_errors.py \
  -q
```

## 4) UI dependency/build checks

```bash
pushd idr_ui
npm ci --ignore-scripts --no-audit
npm run build
popd
```

## 5) Package reproducibility + install smoke

```bash
python -m build
twine check dist/*

python3 -m venv .venv-smoke
source .venv-smoke/bin/activate
python -m pip install dist/*.whl
idr version
idr doctor --json
deactivate

source .venv/bin/activate
```

## 6) Helm chart static checks

```bash
helm lint deployment/helm/idr-enterprise
helm template idr-enterprise deployment/helm/idr-enterprise \
  -f deployment/helm/idr-enterprise/values.example.yaml \
  > /tmp/idr-enterprise-rendered.yaml
test -s /tmp/idr-enterprise-rendered.yaml
```

## 7) Terraform static validation (all providers)

```bash
terraform fmt -recursive deployment/terraform

cd deployment/terraform/aws-eks
terraform init -backend=false
terraform validate

cd ../gcp-gke
terraform init -backend=false
terraform validate

cd ../azure-aks
terraform init -backend=false
terraform validate

cd ../../..
```

## 8) Terraform plan checks with optional DNS automation

Run only for your target cloud account/project/subscription.

### 8A) AWS EKS + Route53 plan

```bash
cd deployment/terraform/aws-eks
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with real values before plan.

terraform plan \
  -var region=us-east-1 \
  -var ingress_hostname=idr.example.com \
  -var create_dns_record=true \
  -var route53_zone_id=Z1234567890ABC
```

### 8B) GCP GKE + Cloud DNS plan

```bash
cd deployment/terraform/gcp-gke
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with real values before plan.

terraform plan \
  -var project_id=my-gcp-project \
  -var region=us-central1 \
  -var zone=us-central1-a \
  -var ingress_hostname=idr.example.com \
  -var create_dns_record=true \
  -var dns_managed_zone=example-com
```

### 8C) Azure AKS + Azure DNS plan

```bash
cd deployment/terraform/azure-aks
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with real values before plan.

terraform plan \
  -var resource_group_name=idr-rg \
  -var location=eastus \
  -var ingress_hostname=idr.example.com \
  -var create_dns_record=true \
  -var dns_zone_name=example.com \
  -var dns_zone_resource_group=shared-dns-rg
```

Return to repo root:

```bash
cd ../../..
```

## 9) Optional enterprise runtime checks (Docker)

```bash
bash tools/deploy/enterprise_up.sh docker-compose.enterprise.yml
bash tools/ci/verify_enterprise_stack.sh docker-compose.enterprise.yml

EXPECT_HEALTHY_SERVICES=api-a,api-b,api,redis,keycloak \
  bash tools/ci/verify_enterprise_stack.sh docker-compose.enterprise.ha.yml
```

## 10) Optional post-deploy API/cluster checks

```bash
export IDR_TOKEN="<bearer-token>"
idr doctor --target cluster \
  --api-url "https://idr.example.com/api/health" \
  --metrics-url "https://idr.example.com/metrics" \
  --whoami-url "https://idr.example.com/api/auth/whoami" \
  --token-env IDR_TOKEN \
  --json
```

## 11) What to share back

```bash
echo "LOG_FILE=$LOG_FILE"
tail -n 120 "$LOG_FILE"
```

Share:
- `LOG_FILE` path
- any failed command section from the log
- Terraform provider used (AWS/GCP/Azure)
- if Terraform plan succeeded and whether DNS resources appeared in the plan
