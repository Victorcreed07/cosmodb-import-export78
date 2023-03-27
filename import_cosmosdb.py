from azure.cosmos import CosmosClient, PartitionKey
import json
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from PyQt5.QtWidgets import QApplication, QFileDialog

load_dotenv()

url = os.getenv("COSMOS_IMPORT_URL")
key = os.getenv("COSMOS_IMPORT_KEY")
database_name = os.getenv("COSMOS_IMPORT_DB_NAME")
client = CosmosClient(url, credential=key)
db = client.create_database_if_not_exists(database_name)

# Define a fallback partition key
fallback_partition_key = "_partitionKey"

# Use PyQt5 to open a directory selection dialog
app = QApplication([])
export_folder = QFileDialog.getExistingDirectory(None, "Select the export folder")
if not export_folder:
    print("No folder selected. Exiting.")
    exit()

def upsert_item(container, item):
    try:
        container.upsert_item(item)
    except Exception as e:
        print(f"Failed to upsert item in container '{container.id}': {e}")

def import_container(file):
    if file.endswith("_export.json"):
        container_name = file[:-12]

        # Use the fallback partition key
        partition_key_path = f"/{fallback_partition_key}"
        container_properties = {
            "id": container_name,
            "partition_key": PartitionKey(path=partition_key_path)
        }

        try:
            container = db.create_container_if_not_exists(**container_properties)
        except Exception as e:
            print(f"Failed to create container '{container_name}': {e}")
            return

        with open(os.path.join(export_folder, file), "r") as f:
            items = json.load(f)
        # start from element 3900
        with ThreadPoolExecutor() as item_executor:
            item_futures = [item_executor.submit(upsert_item, container, item) for item in items]

            # Use tqdm for progress bar
            for _ in tqdm(as_completed(item_futures), total=len(items), desc=f"Importing {container_name}", unit=" items"):
                pass

                
# Use ThreadPoolExecutor for parallel import
with ThreadPoolExecutor() as executor:
    executor.map(import_container, os.listdir(export_folder))