# AWS EKS Full Provisioning

This stack provisions:
- VPC (optional) with public/private subnets
- EKS cluster (optional) with managed node group
- AWS Secrets Manager secret for runtime credentials
- Kubernetes namespace + secret for IDR chart
- NGINX ingress controller + IDR ingress
- `idr-enterprise` Helm release

Input checks enforce attach-mode prerequisites:
- `create_network=false`: requires `existing_vpc_id`, `existing_private_subnet_ids`, `existing_public_subnet_ids`
- `create_cluster=false`: requires `existing_cluster_name`
- `external_oidc_enabled=true`: requires issuer + JWKS URL
- `create_dns_record=true`: requires `create_ingress=true` and `route53_zone_id`

## Quick Start

```bash
cd deployment/terraform/aws-eks
terraform init
terraform apply \
  -var region=us-east-1 \
  -var ingress_hostname=idr.example.com
```

To attach to existing VPC/cluster instead of creating:

```bash
terraform apply \
  -var region=us-east-1 \
  -var create_network=false \
  -var existing_vpc_id=vpc-123 \
  -var 'existing_private_subnet_ids=["subnet-a","subnet-b","subnet-c"]' \
  -var 'existing_public_subnet_ids=["subnet-d","subnet-e","subnet-f"]' \
  -var create_cluster=false \
  -var existing_cluster_name=my-existing-eks
```

For external IdP:

```bash
terraform apply \
  -var region=us-east-1 \
  -var external_oidc_enabled=true \
  -var external_oidc_issuer=https://idp.example.com/realms/idr \
  -var external_oidc_jwks_url=https://idp.example.com/realms/idr/protocol/openid-connect/certs
```

For optional Route53 DNS automation:

```bash
terraform apply \
  -var region=us-east-1 \
  -var ingress_hostname=idr.example.com \
  -var create_dns_record=true \
  -var route53_zone_id=Z1234567890ABC
```

If the ingress load balancer hostname is not yet available during first apply, re-apply once ingress is ready or provide `dns_target_override`.

Local static validation:

```bash
terraform init -backend=false
terraform validate
```
