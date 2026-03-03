output "cluster_name" {
  value = local.target_cluster_name
}

output "cluster_endpoint" {
  value = data.aws_eks_cluster.target.endpoint
}

output "namespace" {
  value = var.namespace
}

output "release_name" {
  value = helm_release.idr_enterprise.name
}

output "ingress_hostname" {
  value = var.ingress_hostname
}

output "kubernetes_secret_name" {
  value = local.effective_secret_name
}

output "secrets_manager_arn" {
  value       = var.create_secret_manager ? aws_secretsmanager_secret.idr[0].arn : ""
  description = "ARN of created AWS Secrets Manager secret"
}

output "dns_record_name" {
  value       = var.create_dns_record ? aws_route53_record.idr[0].fqdn : ""
  description = "Route53 DNS record FQDN when DNS automation is enabled"
}

output "dns_record_target" {
  value       = var.create_dns_record ? local.route53_record_target : ""
  description = "Route53 DNS target value used by Terraform"
}
