# Base Variables
variable "location" {
  description = "The location/region of the resource"
  type = string
  default = "eastus"
}
variable "resource_group_name" {
  description = "The name of the resource group"
  type = string
}

# Data factory variables
# Data Factory Base
variable "adf_name" {
  description = "The name of the Azure data factory resource"
  type = string
}

# Data Factory properties
variable "storage_account_name" {
  description = "the storage account name"
  type = string
}