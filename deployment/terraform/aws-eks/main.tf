provider "aws" {
  region = var.region
}

data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  vpc_az_count = min(
    length(data.aws_availability_zones.available.names),
    length(var.private_subnet_cidrs),
    length(var.public_subnet_cidrs),
  )
  vpc_azs                  = slice(data.aws_availability_zones.available.names, 0, local.vpc_az_count)
  vpc_private_subnet_cidrs = slice(var.private_subnet_cidrs, 0, local.vpc_az_count)
  vpc_public_subnet_cidrs  = slice(var.public_subnet_cidrs, 0, local.vpc_az_count)
}

module "vpc" {
  count   = var.create_network ? 1 : 0
  source  = "terraform-aws-modules/vpc/aws"
  version = "5.8.1"

  name = "${var.project_name}-${var.environment}-vpc"
  cidr = var.vpc_cidr

  azs             = local.vpc_azs
  private_subnets = local.vpc_private_subnet_cidrs
  public_subnets  = local.vpc_public_subnet_cidrs

  enable_nat_gateway = true
  single_nat_gateway = true

  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
  }
}

locals {
  name_prefix        = "${var.project_name}-${var.environment}"
  vpc_id             = var.create_network ? module.vpc[0].vpc_id : var.existing_vpc_id
  private_subnet_ids = var.create_network ? module.vpc[0].private_subnets : var.existing_private_subnet_ids
  public_subnet_ids  = var.create_network ? module.vpc[0].public_subnets : var.existing_public_subnet_ids

  target_cluster_name = var.create_cluster ? module.eks[0].cluster_name : var.existing_cluster_name

  generated_k8s_secret_name = "${var.release_name}-secrets"
  effective_secret_name = var.use_existing_secret_name != "" ? var.use_existing_secret_name : local.generated_k8s_secret_name

  provided_keycloak_password = coalesce(var.keycloak_admin_password, "")
  provided_grafana_password = coalesce(var.grafana_admin_password, "")

  resolved_keycloak_password = trimspace(local.provided_keycloak_password) != "" ? local.provided_keycloak_password : random_password.keycloak_admin.result
  resolved_grafana_password  = trimspace(local.provided_grafana_password) != "" ? local.provided_grafana_password : random_password.grafana_admin.result

  resolved_webhook_token = var.run_job_webhook_bearer_token

  resolved_secret_manager_name = var.secrets_manager_name != "" ? var.secrets_manager_name : "${local.name_prefix}/idr-enterprise"
}

check "aws_vpc_subnet_alignment" {
  assert {
    condition = !var.create_network || (
      local.vpc_az_count >= 2 &&
      length(var.private_subnet_cidrs) == length(var.public_subnet_cidrs)
    )
    error_message = "When create_network=true, provide equal private/public subnet CIDR lists and at least 2 AZs."
  }
}

check "aws_existing_network_inputs" {
  assert {
    condition = var.create_network || (
      trimspace(var.existing_vpc_id) != "" &&
      length(var.existing_private_subnet_ids) > 0 &&
      length(var.existing_public_subnet_ids) > 0
    )
    error_message = "When create_network=false, set existing_vpc_id, existing_private_subnet_ids, and existing_public_subnet_ids."
  }
}

check "aws_existing_cluster_inputs" {
  assert {
    condition     = var.create_cluster || trimspace(var.existing_cluster_name) != ""
    error_message = "When create_cluster=false, set existing_cluster_name."
  }
}

check "aws_ingress_hostname" {
  assert {
    condition     = !var.create_ingress || trimspace(var.ingress_hostname) != ""
    error_message = "When create_ingress=true, ingress_hostname must be set."
  }
}

check "aws_external_oidc_inputs" {
  assert {
    condition = !var.external_oidc_enabled || (
      trimspace(var.external_oidc_issuer) != "" &&
      trimspace(var.external_oidc_jwks_url) != ""
    )
    error_message = "When external_oidc_enabled=true, set external_oidc_issuer and external_oidc_jwks_url."
  }
}

check "aws_dns_inputs" {
  assert {
    condition = !var.create_dns_record || (
      var.create_ingress &&
      trimspace(var.route53_zone_id) != ""
    )
    error_message = "When create_dns_record=true, enable ingress and set route53_zone_id."
  }
}

module "eks" {
  count   = var.create_cluster ? 1 : 0
  source  = "terraform-aws-modules/eks/aws"
  version = "20.12.0"

  cluster_name    = var.cluster_name
  cluster_version = var.kubernetes_version

  vpc_id     = local.vpc_id
  subnet_ids = local.private_subnet_ids

  enable_cluster_creator_admin_permissions = true
  cluster_endpoint_public_access           = true

  cluster_addons = {
    coredns = {}
    kube-proxy = {}
    vpc-cni = {}
  }

  eks_managed_node_groups = {
    default = {
      instance_types = var.node_instance_types
      min_size       = var.node_min_size
      max_size       = var.node_max_size
      desired_size   = var.node_desired_size
    }
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

data "aws_eks_cluster" "target" {
  name = local.target_cluster_name
  depends_on = [module.eks]
}

data "aws_eks_cluster_auth" "target" {
  name = local.target_cluster_name
  depends_on = [module.eks]
}

provider "kubernetes" {
  host                   = data.aws_eks_cluster.target.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.target.certificate_authority[0].data)
  token                  = data.aws_eks_cluster_auth.target.token
}

provider "helm" {
  kubernetes {
    host                   = data.aws_eks_cluster.target.endpoint
    cluster_ca_certificate = base64decode(data.aws_eks_cluster.target.certificate_authority[0].data)
    token                  = data.aws_eks_cluster_auth.target.token
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

resource "aws_secretsmanager_secret" "idr" {
  count = var.create_secret_manager ? 1 : 0

  name        = local.resolved_secret_manager_name
  description = "IDR enterprise runtime secrets"

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_secretsmanager_secret_version" "idr" {
  count = var.create_secret_manager ? 1 : 0

  secret_id = aws_secretsmanager_secret.idr[0].id
  secret_string = jsonencode({
    KEYCLOAK_ADMIN_PASSWORD        = local.resolved_keycloak_password
    GF_SECURITY_ADMIN_PASSWORD     = local.resolved_grafana_password
    IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN = local.resolved_webhook_token
  })
}

data "aws_secretsmanager_secret_version" "idr" {
  count = var.create_secret_manager ? 1 : 0

  secret_id  = aws_secretsmanager_secret.idr[0].id
  depends_on = [aws_secretsmanager_secret_version.idr]
}

locals {
  kubernetes_secret_values = var.create_secret_manager ? jsondecode(data.aws_secretsmanager_secret_version.idr[0].secret_string) : {
    KEYCLOAK_ADMIN_PASSWORD           = local.resolved_keycloak_password
    GF_SECURITY_ADMIN_PASSWORD        = local.resolved_grafana_password
    IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN  = local.resolved_webhook_token
  }

  helm_values_files = concat([file(var.preset_file)], [for v in var.additional_values_files : file(v)])

  helm_set_values = merge(
    {
      "secrets.create"             = "false"
      "secrets.existingSecretName" = local.effective_secret_name
    },
    var.external_oidc_enabled ? {
      "keycloak.enabled"       = "false"
      "api.oidc.issuer"        = var.external_oidc_issuer
      "api.oidc.jwksUrl"       = var.external_oidc_jwks_url
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
    KEYCLOAK_ADMIN_PASSWORD         = local.kubernetes_secret_values.KEYCLOAK_ADMIN_PASSWORD
    GF_SECURITY_ADMIN_PASSWORD      = local.kubernetes_secret_values.GF_SECURITY_ADMIN_PASSWORD
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
      "kubernetes.io/ingress.class"                 = "nginx"
      "nginx.ingress.kubernetes.io/proxy-body-size" = "16m"
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
  dns_record_fqdn      = trimspace(var.dns_record_name) != "" ? trimspace(var.dns_record_name) : var.ingress_hostname
  route53_record_target = trimspace(var.dns_target_override) != "" ? trimspace(var.dns_target_override) : try(kubernetes_ingress_v1.idr[0].status[0].load_balancer[0].ingress[0].hostname, "")
}

resource "aws_route53_record" "idr" {
  count = var.create_dns_record ? 1 : 0

  zone_id = var.route53_zone_id
  name    = local.dns_record_fqdn
  type    = "CNAME"
  ttl     = var.dns_ttl
  records = [local.route53_record_target]

  lifecycle {
    precondition {
      condition     = trimspace(local.route53_record_target) != ""
      error_message = "Ingress load balancer hostname not ready. Re-apply after ingress is provisioned, or set dns_target_override."
    }
  }

  depends_on = [kubernetes_ingress_v1.idr]
}
