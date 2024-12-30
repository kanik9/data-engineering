# Storage Account Variables
variable "account_replication_type" {
  description = "Storage Account replication type"
  type = string
  default = "LRS"
}

variable "account_tier" {
  description = "Storage Account tier"
  type = string
  default = "Standard"
}

variable "location" {
  description = "The Location/Region in which we need to create storage account"
  type = string
}

variable "name" {
  description = "The name of the Storage Account"
  type = string
}

variable "resource_group_name" {
  description = "The name of the resource group"
  type = string
}

# Storage Account - Container Variables
variable "source_folder_name" {
  description = "Name of the source data folder"
  type = string
}

variable "destination_folder_name" {
  description = "Name of the destination data folder"
  type = string
}

variable "container_access_type" {
  description = ""
  type = string
  default = "private"
}
#
