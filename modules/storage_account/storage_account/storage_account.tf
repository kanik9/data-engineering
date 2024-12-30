#
resource "azurerm_storage_account" "storage" {
  account_replication_type = var.account_replication_type # "LRS"
  account_tier             = var.account_tier # "Standard"
  location                 = var.location
  name                     = var.name
  resource_group_name      = var.resource_group_name
  tags                     = {
    environment = "development"
  }
}

#Create Source and Destination container name in the storage account using for each look
resource "azurerm_storage_container" "create_container" {
  for_each = {
    source        = var.source_folder_name
    destination   = var.destination_folder_name
  }
  name = each.key
  storage_account_name = azurerm_storage_account.storage.name

  container_access_type = var.container_access_type
}

resource "azurerm_storage_blob" "create_test_file" {
  name                   = "test.txt"
  storage_account_name   = azurerm_storage_account.storage.name
  storage_container_name = azurerm_storage_container.create_container["source"].name
  type                   = "Block"
  source_content = "Hello Community from Kanik"
}

output "storage_account_key" {
  value = azurerm_storage_account.storage.primary_access_key
}