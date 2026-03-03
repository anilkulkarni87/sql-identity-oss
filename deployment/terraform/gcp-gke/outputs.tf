output "cluster_name" {
  value = local.target_cluster_name
}

output "cluster_endpoint" {
  value = data.google_container_cluster.target.endpoint
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

output "secret_manager_secret_ids" {
  value       = var.create_secret_manager ? [for s in google_secret_manager_secret.idr : s.id] : []
  description = "Created Secret Manager secret IDs"
}

output "dns_record_name" {
  value       = var.create_dns_record ? google_dns_record_set.idr[0].name : ""
  description = "Cloud DNS record FQDN when DNS automation is enabled"
}

output "dns_record_target" {
  value       = var.create_dns_record ? local.dns_record_ip : ""
  description = "Cloud DNS record IP target used by Terraform"
}
