# Azure AKS Full Provisioning

This stack provisions:
- Resource group (optional)
- VNet + subnet (optional)
- AKS cluster (optional)
- Azure Key Vault secrets for runtime credentials
- Kubernetes namespace + secret for IDR chart
- NGINX ingress controller + IDR ingress
- `idr-enterprise` Helm release

Input checks enforce attach-mode prerequisites:
- `create_network=false`: requires `existing_subnet_id`
- `create_cluster=false`: requires `existing_cluster_name`
- `external_oidc_enabled=true`: requires issuer + JWKS URL
- `create_dns_record=true`: requires `create_ingress=true` and `dns_zone_name`

## Quick Start

```bash
cd deployment/terraform/azure-aks
terraform init
terraform apply \
  -var resource_group_name=idr-rg \
  -var location=eastus \
  -var ingress_hostname=idr.example.com
```

To attach to existing AKS/network:

```bash
terraform apply \
  -var create_resource_group=false \
  -var resource_group_name=existing-rg \
  -var create_network=false \
  -var existing_subnet_id=/subscriptions/.../subnets/aks-subnet \
  -var create_cluster=false \
  -var existing_cluster_name=my-existing-aks
```

For external IdP:

```bash
terraform apply \
  -var external_oidc_enabled=true \
  -var external_oidc_issuer=https://idp.example.com/realms/idr \
  -var external_oidc_jwks_url=https://idp.example.com/realms/idr/protocol/openid-connect/certs
```

For optional Azure DNS automation:

```bash
terraform apply \
  -var ingress_hostname=idr.example.com \
  -var create_dns_record=true \
  -var dns_zone_name=example.com \
  -var dns_zone_resource_group=shared-dns-rg
```

If the ingress load balancer IP is not yet available during first apply, re-apply once ingress is ready or provide `dns_target_override`.

Local static validation:

```bash
terraform init -backend=false
terraform validate
```
