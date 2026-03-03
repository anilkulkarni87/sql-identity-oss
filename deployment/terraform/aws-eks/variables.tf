variable "region" {
  description = "AWS region for infrastructure"
  type        = string
}

variable "project_name" {
  description = "Project name prefix"
  type        = string
  default     = "idr"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "prod"
}

variable "create_network" {
  description = "Create VPC and subnets"
  type        = bool
  default     = true
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.42.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "Private subnet CIDRs"
  type        = list(string)
  default     = ["10.42.1.0/24", "10.42.2.0/24", "10.42.3.0/24"]
}

variable "public_subnet_cidrs" {
  description = "Public subnet CIDRs"
  type        = list(string)
  default     = ["10.42.101.0/24", "10.42.102.0/24", "10.42.103.0/24"]
}

variable "existing_vpc_id" {
  description = "Existing VPC ID when create_network=false"
  type        = string
  default     = ""
}

variable "existing_private_subnet_ids" {
  description = "Existing private subnet IDs when create_network=false"
  type        = list(string)
  default     = []
}

variable "existing_public_subnet_ids" {
  description = "Existing public subnet IDs when create_network=false"
  type        = list(string)
  default     = []
}

variable "create_cluster" {
  description = "Create EKS cluster"
  type        = bool
  default     = true
}

variable "cluster_name" {
  description = "EKS cluster name when create_cluster=true"
  type        = string
  default     = "idr-eks"
}

variable "existing_cluster_name" {
  description = "Existing EKS cluster name when create_cluster=false"
  type        = string
  default     = ""
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version"
  type        = string
  default     = "1.29"
}

variable "node_instance_types" {
  description = "Managed node group instance types"
  type        = list(string)
  default     = ["m6i.large"]
}

variable "node_min_size" {
  description = "Managed node group minimum size"
  type        = number
  default     = 2
}

variable "node_max_size" {
  description = "Managed node group maximum size"
  type        = number
  default     = 6
}

variable "node_desired_size" {
  description = "Managed node group desired size"
  type        = number
  default     = 3
}

variable "namespace" {
  description = "Kubernetes namespace"
  type        = string
  default     = "idr"
}

variable "release_name" {
  description = "Helm release name"
  type        = string
  default     = "idr-enterprise"
}

variable "chart_path" {
  description = "Path to idr-enterprise Helm chart"
  type        = string
  default     = "../../helm/idr-enterprise"
}

variable "preset_file" {
  description = "Provider preset values file"
  type        = string
  default     = "../../helm/presets/aws-eks.yaml"
}

variable "additional_values_files" {
  description = "Additional Helm values files"
  type        = list(string)
  default     = []
}

variable "additional_set_values" {
  description = "Additional Helm set overrides"
  type        = map(string)
  default     = {}
}

variable "create_ingress" {
  description = "Create nginx ingress controller and Ingress"
  type        = bool
  default     = true
}

variable "ingress_hostname" {
  description = "Hostname for UI/API ingress"
  type        = string
  default     = "idr.example.com"
}

variable "ingress_tls_secret_name" {
  description = "Optional TLS secret name"
  type        = string
  default     = ""
}

variable "create_dns_record" {
  description = "Create Route53 DNS record for ingress hostname"
  type        = bool
  default     = false
}

variable "route53_zone_id" {
  description = "Route53 hosted zone ID for DNS record creation"
  type        = string
  default     = ""
}

variable "dns_record_name" {
  description = "Optional Route53 record name override (defaults to ingress_hostname)"
  type        = string
  default     = ""
}

variable "dns_ttl" {
  description = "DNS TTL in seconds"
  type        = number
  default     = 300
}

variable "dns_target_override" {
  description = "Optional DNS target override (uses ingress load balancer hostname when empty)"
  type        = string
  default     = ""
}

variable "create_secret_manager" {
  description = "Create AWS Secrets Manager secret for runtime credentials"
  type        = bool
  default     = true
}

variable "secrets_manager_name" {
  description = "Secrets Manager secret name"
  type        = string
  default     = ""
}

variable "keycloak_admin_password" {
  description = "Optional Keycloak admin password override"
  type        = string
  default     = null
  sensitive   = true
}

variable "grafana_admin_password" {
  description = "Optional Grafana admin password override"
  type        = string
  default     = null
  sensitive   = true
}

variable "run_job_webhook_bearer_token" {
  description = "Optional webhook bearer token"
  type        = string
  default     = ""
  sensitive   = true
}

variable "use_existing_secret_name" {
  description = "Existing Kubernetes secret name for chart secret references"
  type        = string
  default     = ""
}

variable "external_oidc_enabled" {
  description = "Disable bundled Keycloak and use external OIDC"
  type        = bool
  default     = false
}

variable "external_oidc_issuer" {
  description = "External OIDC issuer URL"
  type        = string
  default     = ""
}

variable "external_oidc_jwks_url" {
  description = "External OIDC JWKS URL"
  type        = string
  default     = ""
}

variable "external_oidc_audience" {
  description = "External OIDC audience"
  type        = string
  default     = "account"
}
