provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_client_config" "current" {}

resource "google_compute_network" "idr" {
  count                   = var.create_network ? 1 : 0
  name                    = "${var.project_name}-${var.environment}-${var.vpc_name}"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "idr" {
  count = var.create_network ? 1 : 0

  name          = "${var.project_name}-${var.environment}-${var.subnet_name}"
  ip_cidr_range = var.subnet_cidr
  region        = var.region
  network       = google_compute_network.idr[0].id

  secondary_ip_range {
    range_name    = "pods"
    ip_cidr_range = "10.53.0.0/16"
  }

  secondary_ip_range {
    range_name    = "services"
    ip_cidr_range = "10.54.0.0/20"
  }
}

locals {
  target_cluster_location = var.cluster_location != "" ? var.cluster_location : var.zone
  network_name            = var.create_network ? google_compute_network.idr[0].name : var.existing_network_name
  subnetwork_name         = var.create_network ? google_compute_subnetwork.idr[0].name : var.existing_subnetwork_name

  target_cluster_name = var.create_cluster ? google_container_cluster.idr[0].name : var.existing_cluster_name

  generated_k8s_secret_name = "${var.release_name}-secrets"
  effective_secret_name = var.use_existing_secret_name != "" ? var.use_existing_secret_name : local.generated_k8s_secret_name

  provided_keycloak_password = coalesce(var.keycloak_admin_password, "")
  provided_grafana_password = coalesce(var.grafana_admin_password, "")

  resolved_keycloak_password = trimspace(local.provided_keycloak_password) != "" ? local.provided_keycloak_password : random_password.keycloak_admin.result
  resolved_grafana_password  = trimspace(local.provided_grafana_password) != "" ? local.provided_grafana_password : random_password.grafana_admin.result

  resolved_webhook_token = var.run_job_webhook_bearer_token

  secret_payload = {
    KEYCLOAK_ADMIN_PASSWORD          = local.resolved_keycloak_password
    GF_SECURITY_ADMIN_PASSWORD       = local.resolved_grafana_password
    IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN = local.resolved_webhook_token
  }

  dns_project_id = trimspace(var.dns_project_id) != "" ? trimspace(var.dns_project_id) : var.project_id
}

check "gcp_existing_network_inputs" {
  assert {
    condition = var.create_network || (
      trimspace(var.existing_network_name) != "" &&
      trimspace(var.existing_subnetwork_name) != ""
    )
    error_message = "When create_network=false, set existing_network_name and existing_subnetwork_name."
  }
}

check "gcp_existing_cluster_inputs" {
  assert {
    condition     = var.create_cluster || trimspace(var.existing_cluster_name) != ""
    error_message = "When create_cluster=false, set existing_cluster_name."
  }
}

check "gcp_ingress_hostname" {
  assert {
    condition     = !var.create_ingress || trimspace(var.ingress_hostname) != ""
    error_message = "When create_ingress=true, ingress_hostname must be set."
  }
}

check "gcp_external_oidc_inputs" {
  assert {
    condition = !var.external_oidc_enabled || (
      trimspace(var.external_oidc_issuer) != "" &&
      trimspace(var.external_oidc_jwks_url) != ""
    )
    error_message = "When external_oidc_enabled=true, set external_oidc_issuer and external_oidc_jwks_url."
  }
}

check "gcp_dns_inputs" {
  assert {
    condition = !var.create_dns_record || (
      var.create_ingress &&
      trimspace(var.dns_managed_zone) != ""
    )
    error_message = "When create_dns_record=true, enable ingress and set dns_managed_zone."
  }
}

resource "google_container_cluster" "idr" {
  count = var.create_cluster ? 1 : 0

  name               = var.cluster_name
  location           = local.target_cluster_location
  min_master_version = var.kubernetes_version

  network    = local.network_name
  subnetwork = local.subnetwork_name

  remove_default_node_pool = true
  initial_node_count       = 1

  release_channel {
    channel = "REGULAR"
  }
}

resource "google_container_node_pool" "idr" {
  count = var.create_cluster ? 1 : 0

  name       = "${var.cluster_name}-default"
  location   = local.target_cluster_location
  cluster    = google_container_cluster.idr[0].name
  node_count = var.node_count
  version    = var.kubernetes_version

  node_config {
    machine_type = var.node_machine_type
    oauth_scopes = [
      "https://www.googleapis.com/auth/cloud-platform",
    ]
    labels = {
      project     = var.project_name
      environment = var.environment
    }
  }
}

data "google_container_cluster" "target" {
  name     = local.target_cluster_name
  location = local.target_cluster_location
  project  = var.project_id
  depends_on = [google_container_cluster.idr]
}

provider "kubernetes" {
  host  = "https://${data.google_container_cluster.target.endpoint}"
  token = data.google_client_config.current.access_token

  cluster_ca_certificate = base64decode(
    data.google_container_cluster.target.master_auth[0].cluster_ca_certificate
  )
}

provider "helm" {
  kubernetes {
    host  = "https://${data.google_container_cluster.target.endpoint}"
    token = data.google_client_config.current.access_token

    cluster_ca_certificate = base64decode(
      data.google_container_cluster.target.master_auth[0].cluster_ca_certificate
    )
  }
}

resource "random_password" "keycloak_admin" {
  length  = 32
  special = false
}

resource "random_password" "grafana_admin" {
  length  = 32
  special = false
}

resource "google_secret_manager_secret" "idr" {
  for_each = var.create_secret_manager ? local.secret_payload : {}

  secret_id = "${var.project_name}-${var.environment}-${lower(each.key)}"
  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "idr" {
  for_each = var.create_secret_manager ? local.secret_payload : {}

  secret      = google_secret_manager_secret.idr[each.key].id
  secret_data = each.value
}

data "google_secret_manager_secret_version" "idr" {
  for_each = var.create_secret_manager ? local.secret_payload : {}

  secret  = google_secret_manager_secret.idr[each.key].id
  version = "latest"
  depends_on = [google_secret_manager_secret_version.idr]
}

locals {
  kubernetes_secret_values = var.create_secret_manager ? {
    for k, _v in local.secret_payload :
    k => data.google_secret_manager_secret_version.idr[k].secret_data
  } : local.secret_payload

  helm_values_files = concat([file(var.preset_file)], [for v in var.additional_values_files : file(v)])

  helm_set_values = merge(
    {
      "secrets.create"             = "false"
      "secrets.existingSecretName" = local.effective_secret_name
    },
    var.external_oidc_enabled ? {
      "keycloak.enabled"          = "false"
      "api.oidc.issuer"           = var.external_oidc_issuer
      "api.oidc.jwksUrl"          = var.external_oidc_jwks_url
      "api.env.IDR_AUTH_AUDIENCE" = var.external_oidc_audience
    } : {},
    var.additional_set_values,
  )
}

resource "kubernetes_namespace" "idr" {
  metadata {
    name = var.namespace
  }
}

resource "kubernetes_secret" "idr_enterprise" {
  count = var.use_existing_secret_name == "" ? 1 : 0

  metadata {
    name      = local.effective_secret_name
    namespace = kubernetes_namespace.idr.metadata[0].name
  }

  type = "Opaque"

  data = {
    KEYCLOAK_ADMIN_PASSWORD          = local.kubernetes_secret_values.KEYCLOAK_ADMIN_PASSWORD
    GF_SECURITY_ADMIN_PASSWORD       = local.kubernetes_secret_values.GF_SECURITY_ADMIN_PASSWORD
    IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN = local.kubernetes_secret_values.IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN
  }
}

resource "helm_release" "ingress_nginx" {
  count = var.create_ingress ? 1 : 0

  name             = "ingress-nginx"
  namespace        = "ingress-nginx"
  create_namespace = true
  repository       = "https://kubernetes.github.io/ingress-nginx"
  chart            = "ingress-nginx"
  version          = "4.11.1"
}

resource "helm_release" "idr_enterprise" {
  name             = var.release_name
  namespace        = kubernetes_namespace.idr.metadata[0].name
  create_namespace = false
  chart            = var.chart_path

  values = local.helm_values_files

  dynamic "set" {
    for_each = local.helm_set_values
    content {
      name  = set.key
      value = set.value
    }
  }

  depends_on = [
    kubernetes_namespace.idr,
    kubernetes_secret.idr_enterprise,
    helm_release.ingress_nginx,
  ]
}

resource "kubernetes_ingress_v1" "idr" {
  count = var.create_ingress ? 1 : 0

  metadata {
    name      = "${var.release_name}-ingress"
    namespace = kubernetes_namespace.idr.metadata[0].name
    annotations = {
      "kubernetes.io/ingress.class" = "nginx"
    }
  }

  spec {
    ingress_class_name = "nginx"

    rule {
      host = var.ingress_hostname
      http {
        path {
          path      = "/api"
          path_type = "Prefix"
          backend {
            service {
              name = "${var.release_name}-api"
              port {
                number = 8000
              }
            }
          }
        }

        path {
          path      = "/"
          path_type = "Prefix"
          backend {
            service {
              name = "${var.release_name}-ui"
              port {
                number = 80
              }
            }
          }
        }
      }
    }

    dynamic "tls" {
      for_each = var.ingress_tls_secret_name != "" ? [1] : []
      content {
        hosts       = [var.ingress_hostname]
        secret_name = var.ingress_tls_secret_name
      }
    }
  }

  depends_on = [helm_release.idr_enterprise]
}

locals {
  dns_record_fqdn = trimspace(var.dns_record_name) != "" ? trimspace(var.dns_record_name) : var.ingress_hostname
  dns_record_ip   = trimspace(var.dns_target_override) != "" ? trimspace(var.dns_target_override) : try(kubernetes_ingress_v1.idr[0].status[0].load_balancer[0].ingress[0].ip, "")
}

resource "google_dns_record_set" "idr" {
  count = var.create_dns_record ? 1 : 0

  project      = local.dns_project_id
  managed_zone = var.dns_managed_zone
  name         = "${trimsuffix(local.dns_record_fqdn, ".")}."
  type         = "A"
  ttl          = var.dns_ttl
  rrdatas      = [local.dns_record_ip]

  lifecycle {
    precondition {
      condition     = trimspace(local.dns_record_ip) != ""
      error_message = "Ingress load balancer IP not ready. Re-apply after ingress is provisioned, or set dns_target_override."
    }
  }

  depends_on = [kubernetes_ingress_v1.idr]
}
