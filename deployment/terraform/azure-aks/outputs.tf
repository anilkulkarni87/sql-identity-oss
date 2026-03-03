output "cluster_name" {
  value = local.target_cluster_name
}

output "cluster_host" {
  value = local.kube_host
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

output "key_vault_id" {
  value       = var.create_secret_manager ? azurerm_key_vault.idr[0].id : ""
  description = "Created Key Vault ID"
}

output "dns_record_name" {
  value       = var.create_dns_record ? azurerm_dns_a_record.idr[0].fqdn : ""
  description = "Azure DNS record FQDN when DNS automation is enabled"
}

output "dns_record_target" {
  value       = var.create_dns_record ? local.dns_record_ip : ""
  description = "Azure DNS record IP target used by Terraform"
}
