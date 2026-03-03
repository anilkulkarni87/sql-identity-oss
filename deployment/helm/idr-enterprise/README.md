# idr-enterprise Helm Chart

Enterprise self-hosted Kubernetes package for SQL Identity Resolution.

## Install

```bash
helm upgrade --install idr-enterprise deployment/helm/idr-enterprise \
  --namespace idr --create-namespace \
  --set secrets.keycloakAdminPassword='<strong-password>' \
  --set secrets.grafanaAdminPassword='<strong-password>'
```

Cloud preset + existing secret:

```bash
helm upgrade --install idr-enterprise deployment/helm/idr-enterprise \
  --namespace idr --create-namespace \
  -f deployment/helm/presets/aws-eks.yaml \
  --set secrets.create=false \
  --set secrets.existingSecretName=idr-enterprise-secrets
```

## Validate

```bash
helm lint deployment/helm/idr-enterprise
helm template idr-enterprise deployment/helm/idr-enterprise -f deployment/helm/idr-enterprise/values.example.yaml
```

## Notes

- Chart bundles API, UI, Keycloak bootstrap, Redis, Prometheus, and Grafana.
- Secret values in `values.yaml` are placeholders; override at install time.
- `secrets.existingSecretName` lets you reuse externally managed Kubernetes Secrets.
