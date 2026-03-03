# Cloud Presets

Provider baseline values for the `idr-enterprise` Helm chart:

- `aws-eks.yaml`
- `gcp-gke.yaml`
- `azure-aks.yaml`

These presets are intentionally minimal and should be layered with an environment-specific values file.

Example:

```bash
helm upgrade --install idr-enterprise deployment/helm/idr-enterprise \
  --namespace idr --create-namespace \
  -f deployment/helm/presets/aws-eks.yaml \
  -f deployment/helm/idr-enterprise/values.example.yaml
```
