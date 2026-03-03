# GCP GKE Full Provisioning

This stack provisions:
- VPC/subnetwork (optional)
- GKE cluster + node pool (optional)
- Secret Manager secrets for runtime credentials
- Kubernetes namespace + secret for IDR chart
- NGINX ingress controller + IDR ingress
- `idr-enterprise` Helm release

Input checks enforce attach-mode prerequisites:
- `create_network=false`: requires `existing_network_name`, `existing_subnetwork_name`
- `create_cluster=false`: requires `existing_cluster_name`
- `external_oidc_enabled=true`: requires issuer + JWKS URL
- `create_dns_record=true`: requires `create_ingress=true` and `dns_managed_zone`

## Quick Start

```bash
cd deployment/terraform/gcp-gke
terraform init
terraform apply \
  -var project_id=<gcp-project> \
  -var region=us-central1 \
  -var zone=us-central1-a \
  -var ingress_hostname=idr.example.com
```

To attach to existing cluster/network:

```bash
terraform apply \
  -var project_id=<gcp-project> \
  -var create_network=false \
  -var existing_network_name=my-vpc \
  -var existing_subnetwork_name=my-subnet \
  -var create_cluster=false \
  -var existing_cluster_name=my-existing-gke \
  -var cluster_location=us-central1
```

For external IdP:

```bash
terraform apply \
  -var project_id=<gcp-project> \
  -var external_oidc_enabled=true \
  -var external_oidc_issuer=https://idp.example.com/realms/idr \
  -var external_oidc_jwks_url=https://idp.example.com/realms/idr/protocol/openid-connect/certs
```

For optional Cloud DNS automation:

```bash
terraform apply \
  -var project_id=<gcp-project> \
  -var ingress_hostname=idr.example.com \
  -var create_dns_record=true \
  -var dns_managed_zone=example-com
```

If the ingress load balancer IP is not yet available during first apply, re-apply once ingress is ready or provide `dns_target_override`.

Local static validation:

```bash
terraform init -backend=false
terraform validate
```
