variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "zone" {
  description = "GCP zone for node pool"
  type        = string
  default     = "us-central1-a"
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
  description = "Create VPC and subnetwork"
  type        = bool
  default     = true
}

variable "vpc_name" {
  description = "VPC name when create_network=true"
  type        = string
  default     = "idr-vpc"
}

variable "subnet_name" {
  description = "Subnetwork name when create_network=true"
  type        = string
  default     = "idr-subnet"
}

variable "subnet_cidr" {
  description = "Subnetwork CIDR"
  type        = string
  default     = "10.52.0.0/20"
}

variable "existing_network_name" {
  description = "Existing VPC name when create_network=false"
  type        = string
  default     = ""
}

variable "existing_subnetwork_name" {
  description = "Existing subnetwork name when create_network=false"
  type        = string
  default     = ""
}

variable "create_cluster" {
  description = "Create GKE cluster"
  type        = bool
  default     = true
}

variable "cluster_name" {
  description = "GKE cluster name when create_cluster=true"
  type        = string
  default     = "idr-gke"
}

variable "existing_cluster_name" {
  description = "Existing cluster name when create_cluster=false"
  type        = string
  default     = ""
}

variable "cluster_location" {
  description = "Cluster location (region or zone). Empty uses zone."
  type        = string
  default     = ""
}

variable "kubernetes_version" {
  description = "GKE Kubernetes version"
  type        = string
  default     = "1.29"
}

variable "node_count" {
  description = "Node pool count"
  type        = number
  default     = 3
}

variable "node_machine_type" {
  description = "Node pool machine type"
  type        = string
  default     = "e2-standard-4"
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
  default     = "../../helm/presets/gcp-gke.yaml"
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
  description = "Create Cloud DNS record for ingress hostname"
  type        = bool
  default     = false
}

variable "dns_managed_zone" {
  description = "Cloud DNS managed zone name"
  type        = string
  default     = ""
}

variable "dns_project_id" {
  description = "Optional DNS project ID override (defaults to project_id)"
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
  description = "Create GCP Secret Manager secrets"
  type        = bool
  default     = true
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
