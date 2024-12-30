terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
      version = "~>4.14.0"
    }
  }
  required_version = ">=1.1.0"
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
  tags = var.tags
}

module "storage_account" {
  source = "./modules/storage_account/storage_account/"
  resource_group_name = var.resource_group_name
  name = var.name
  location = var.location
  destination_folder_name = var.destination_folder_name
  source_folder_name = var.source_folder_name

  depends_on = [
    azurerm_resource_group.rg
  ]
}

module "data_factory" {
  source = "./modules/data_factory/data_factory/"
  adf_name = var.adf_name
  resource_group_name = var.resource_group_name
  storage_account_name = var.name

  depends_on = [
    module.storage_account
  ]
}