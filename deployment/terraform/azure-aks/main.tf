provider "azurerm" {
  features {}
  subscription_id = var.subscription_id != "" ? var.subscription_id : null
}

data "azurerm_client_config" "current" {}

resource "azurerm_resource_group" "idr" {
  count = var.create_resource_group ? 1 : 0

  name     = var.resource_group_name
  location = var.location
}

locals {
  target_rg_name = var.create_resource_group ? azurerm_resource_group.idr[0].name : var.resource_group_name
}

resource "azurerm_virtual_network" "idr" {
  count = var.create_network ? 1 : 0

  name                = "${var.project_name}-${var.environment}-${var.vnet_name}"
  location            = var.location
  resource_group_name = local.target_rg_name
  address_space       = [var.vnet_cidr]
}

resource "azurerm_subnet" "aks" {
  count = var.create_network ? 1 : 0

  name                 = var.subnet_name
  resource_group_name  = local.target_rg_name
  virtual_network_name = azurerm_virtual_network.idr[0].name
  address_prefixes     = [var.subnet_cidr]
}

locals {
  subnet_id = var.create_network ? azurerm_subnet.aks[0].id : var.existing_subnet_id
}

resource "azurerm_kubernetes_cluster" "idr" {
  count = var.create_cluster ? 1 : 0

  name                = var.cluster_name
  location            = var.location
  resource_group_name = local.target_rg_name
  dns_prefix          = "${var.project_name}-${var.environment}"
  kubernetes_version  = var.kubernetes_version

  default_node_pool {
    name                = "default"
    node_count          = var.node_count
    vm_size             = var.node_vm_size
    vnet_subnet_id      = local.subnet_id
    type                = "VirtualMachineScaleSets"
    enable_auto_scaling = false
  }

  identity {
    type = "SystemAssigned"
  }

  network_profile {
    network_plugin = "azure"
    network_policy = "azure"
  }
}

data "azurerm_kubernetes_cluster" "existing" {
  count = var.create_cluster ? 0 : 1

  name                = var.existing_cluster_name
  resource_group_name = local.target_rg_name
}

locals {
  target_cluster_name = var.create_cluster ? azurerm_kubernetes_cluster.idr[0].name : data.azurerm_kubernetes_cluster.existing[0].name

  kube_host = var.create_cluster ? azurerm_kubernetes_cluster.idr[0].kube_config[0].host : data.azurerm_kubernetes_cluster.existing[0].kube_config[0].host
  kube_client_certificate = var.create_cluster ? azurerm_kubernetes_cluster.idr[0].kube_config[0].client_certificate : data.azurerm_kubernetes_cluster.existing[0].kube_config[0].client_certificate
  kube_client_key = var.create_cluster ? azurerm_kubernetes_cluster.idr[0].kube_config[0].client_key : data.azurerm_kubernetes_cluster.existing[0].kube_config[0].client_key
  kube_cluster_ca_certificate = var.create_cluster ? azurerm_kubernetes_cluster.idr[0].kube_config[0].cluster_ca_certificate : data.azurerm_kubernetes_cluster.existing[0].kube_config[0].cluster_ca_certificate

  generated_k8s_secret_name = "${var.release_name}-secrets"
  effective_secret_name = var.use_existing_secret_name != "" ? var.use_existing_secret_name : local.generated_k8s_secret_name

  provided_keycloak_password = coalesce(var.keycloak_admin_password, "")
  provided_grafana_password = coalesce(var.grafana_admin_password, "")

  resolved_keycloak_password = trimspace(local.provided_keycloak_password) != "" ? local.provided_keycloak_password : random_password.keycloak_admin.result
  resolved_grafana_password  = trimspace(local.provided_grafana_password) != "" ? local.provided_grafana_password : random_password.grafana_admin.result

  resolved_webhook_token = var.run_job_webhook_bearer_token

  resolved_key_vault_name = lower(substr(
    replace(var.key_vault_name != "" ? var.key_vault_name : "${var.project_name}${var.environment}idrvault", "-", ""),
    0,
    24,
  ))

  secret_payload = {
    KEYCLOAK_ADMIN_PASSWORD          = local.resolved_keycloak_password
    GF_SECURITY_ADMIN_PASSWORD       = local.resolved_grafana_password
    IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN = local.resolved_webhook_token
  }

  dns_zone_name_normalized       = trimsuffix(trimspace(var.dns_zone_name), ".")
  dns_record_fqdn                = trimsuffix(trimspace(var.dns_record_name) != "" ? trimspace(var.dns_record_name) : var.ingress_hostname, ".")
  dns_zone_resource_group_name   = trimspace(var.dns_zone_resource_group) != "" ? trimspace(var.dns_zone_resource_group) : local.target_rg_name
  azure_dns_record_relative_name = lower(local.dns_record_fqdn) == lower(local.dns_zone_name_normalized) ? "@" : trimsuffix(local.dns_record_fqdn, ".${local.dns_zone_name_normalized}")
}

check "azure_resource_group_input" {
  assert {
    condition     = trimspace(var.resource_group_name) != ""
    error_message = "resource_group_name must be set."
  }
}

check "azure_existing_network_inputs" {
  assert {
    condition     = var.create_network || trimspace(var.existing_subnet_id) != ""
    error_message = "When create_network=false, set existing_subnet_id."
  }
}

check "azure_existing_cluster_inputs" {
  assert {
    condition     = var.create_cluster || trimspace(var.existing_cluster_name) != ""
    error_message = "When create_cluster=false, set existing_cluster_name."
  }
}

check "azure_ingress_hostname" {
  assert {
    condition     = !var.create_ingress || trimspace(var.ingress_hostname) != ""
    error_message = "When create_ingress=true, ingress_hostname must be set."
  }
}

check "azure_external_oidc_inputs" {
  assert {
    condition = !var.external_oidc_enabled || (
      trimspace(var.external_oidc_issuer) != "" &&
      trimspace(var.external_oidc_jwks_url) != ""
    )
    error_message = "When external_oidc_enabled=true, set external_oidc_issuer and external_oidc_jwks_url."
  }
}

check "azure_dns_inputs" {
  assert {
    condition = !var.create_dns_record || (
      var.create_ingress &&
      trimspace(var.dns_zone_name) != "" &&
      (
        lower(local.dns_record_fqdn) == lower(local.dns_zone_name_normalized) ||
        endswith(lower(local.dns_record_fqdn), ".${lower(local.dns_zone_name_normalized)}")
      )
    )
    error_message = "When create_dns_record=true, enable ingress, set dns_zone_name, and ensure dns_record_name/ingress_hostname belongs to that zone."
  }
}

provider "kubernetes" {
  host                   = local.kube_host
  client_certificate     = base64decode(local.kube_client_certificate)
  client_key             = base64decode(local.kube_client_key)
  cluster_ca_certificate = base64decode(local.kube_cluster_ca_certificate)
}

provider "helm" {
  kubernetes {
    host                   = local.kube_host
    client_certificate     = base64decode(local.kube_client_certificate)
    client_key             = base64decode(local.kube_client_key)
    cluster_ca_certificate = base64decode(local.kube_cluster_ca_certificate)
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

resource "azurerm_key_vault" "idr" {
  count = var.create_secret_manager ? 1 : 0

  name                       = local.resolved_key_vault_name
  location                   = var.location
  resource_group_name        = local.target_rg_name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7
  purge_protection_enabled   = false

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    secret_permissions = [
      "Get",
      "List",
      "Set",
      "Delete",
      "Purge",
      "Recover",
    ]
  }
}

resource "azurerm_key_vault_secret" "idr" {
  for_each = var.create_secret_manager ? local.secret_payload : {}

  name         = lower(replace(each.key, "_", "-"))
  value        = each.value
  key_vault_id = azurerm_key_vault.idr[0].id
}

locals {
  kubernetes_secret_values = local.secret_payload

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
  dns_record_ip = trimspace(var.dns_target_override) != "" ? trimspace(var.dns_target_override) : try(kubernetes_ingress_v1.idr[0].status[0].load_balancer[0].ingress[0].ip, "")
}

resource "azurerm_dns_a_record" "idr" {
  count = var.create_dns_record ? 1 : 0

  name                = local.azure_dns_record_relative_name
  zone_name           = var.dns_zone_name
  resource_group_name = local.dns_zone_resource_group_name
  ttl                 = var.dns_ttl
  records             = [local.dns_record_ip]

  lifecycle {
    precondition {
      condition     = trimspace(local.dns_record_ip) != ""
      error_message = "Ingress load balancer IP not ready. Re-apply after ingress is provisioned, or set dns_target_override."
    }
  }

  depends_on = [kubernetes_ingress_v1.idr]
}
