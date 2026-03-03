# Helm Enterprise Package

Kubernetes deployment package location:
- `deployment/helm/idr-enterprise`

This chart provides:
- API + UI services
- OIDC wiring for API token validation
- Prometheus + Grafana metrics stack
- Keycloak bootstrap (realm import)
- Redis dependency
- Secret-backed runtime credentials

## Quick Start

```bash
helm upgrade --install idr-enterprise deployment/helm/idr-enterprise \
  --namespace idr --create-namespace \
  --set secrets.keycloakAdminPassword='<strong-password>' \
  --set secrets.grafanaAdminPassword='<strong-password>'
```

Cloud preset path:

```bash
helm upgrade --install idr-enterprise deployment/helm/idr-enterprise \
  --namespace idr --create-namespace \
  -f deployment/helm/presets/aws-eks.yaml \
  --set secrets.create=false \
  --set secrets.existingSecretName=idr-enterprise-secrets
```

Deployment wrapper path:

```bash
bash tools/deploy/idr_deploy.sh \
  --provider aws \
  --mode apply \
  --use-existing-secret idr-enterprise-secrets
```

Terraform full provisioning modules:
- `deployment/terraform/aws-eks/main.tf`
- `deployment/terraform/gcp-gke/main.tf`
- `deployment/terraform/azure-aks/main.tf`

Use example values:

```bash
helm upgrade --install idr-enterprise deployment/helm/idr-enterprise \
  --namespace idr --create-namespace \
  -f deployment/helm/idr-enterprise/values.example.yaml
```

## Security Notes

- Do not commit real secret values in values files.
- Prefer external secret management integration and set `secrets.create=false` when wiring existing Kubernetes Secrets.
- Keep `IDR_ALLOW_INSECURE_DEV_AUTH=false` in production.
- Use `tools/deploy/bootstrap_k8s_secrets.sh` to reconcile Kubernetes secrets before Helm apply.

## Validation

CI validates the package with:
- `helm lint deployment/helm/idr-enterprise`
- `helm template ... -f values.example.yaml`
- `bash tools/deploy/idr_deploy.sh --provider aws --mode plan`
