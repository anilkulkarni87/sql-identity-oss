variable "subscription_id" {
  description = "Azure subscription ID (optional if set in environment)"
  type        = string
  default     = ""
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "eastus"
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

variable "create_resource_group" {
  description = "Create resource group"
  type        = bool
  default     = true
}

variable "resource_group_name" {
  description = "Resource group name"
  type        = string
  default     = "idr-rg"
}

variable "create_network" {
  description = "Create VNet and subnet"
  type        = bool
  default     = true
}

variable "vnet_name" {
  description = "VNet name"
  type        = string
  default     = "idr-vnet"
}

variable "vnet_cidr" {
  description = "VNet CIDR"
  type        = string
  default     = "10.62.0.0/16"
}

variable "subnet_name" {
  description = "AKS subnet name"
  type        = string
  default     = "aks-subnet"
}

variable "subnet_cidr" {
  description = "AKS subnet CIDR"
  type        = string
  default     = "10.62.1.0/24"
}

variable "existing_subnet_id" {
  description = "Existing subnet ID when create_network=false"
  type        = string
  default     = ""
}

variable "create_cluster" {
  description = "Create AKS cluster"
  type        = bool
  default     = true
}

variable "cluster_name" {
  description = "AKS cluster name when create_cluster=true"
  type        = string
  default     = "idr-aks"
}

variable "existing_cluster_name" {
  description = "Existing AKS cluster name when create_cluster=false"
  type        = string
  default     = ""
}

variable "kubernetes_version" {
  description = "AKS Kubernetes version"
  type        = string
  default     = "1.29"
}

variable "node_count" {
  description = "Default node count"
  type        = number
  default     = 3
}

variable "node_vm_size" {
  description = "Node VM size"
  type        = string
  default     = "Standard_D4s_v5"
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
  description = "Path to Helm chart"
  type        = string
  default     = "../../helm/idr-enterprise"
}

variable "preset_file" {
  description = "Provider preset values file"
  type        = string
  default     = "../../helm/presets/azure-aks.yaml"
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
  description = "Hostname for ingress"
  type        = string
  default     = "idr.example.com"
}

variable "ingress_tls_secret_name" {
  description = "Optional TLS secret name"
  type        = string
  default     = ""
}

variable "create_dns_record" {
  description = "Create Azure DNS A record for ingress hostname"
  type        = bool
  default     = false
}

variable "dns_zone_name" {
  description = "Azure DNS zone name (for example: example.com)"
  type        = string
  default     = ""
}

variable "dns_zone_resource_group" {
  description = "Resource group containing Azure DNS zone (defaults to resource_group_name)"
  type        = string
  default     = ""
}

variable "dns_record_name" {
  description = "Optional DNS record FQDN override (defaults to ingress_hostname)"
  type        = string
  default     = ""
}

variable "dns_ttl" {
  description = "DNS TTL in seconds"
  type        = number
  default     = 300
}

variable "dns_target_override" {
  description = "Optional DNS A record target IP override (uses ingress load balancer IP when empty)"
  type        = string
  default     = ""
}

variable "create_secret_manager" {
  description = "Create Azure Key Vault and store secrets"
  type        = bool
  default     = true
}

variable "key_vault_name" {
  description = "Key Vault name override"
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
  description = "Existing Kubernetes secret name for chart references"
  type        = string
  default     = ""
}

variable "external_oidc_enabled" {
  description = "Disable bundled Keycloak and use external OIDC"
  type        = bool
  default     = false
}

variable "external_oidc_issuer" {
  description = "External OIDC issuer"
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
