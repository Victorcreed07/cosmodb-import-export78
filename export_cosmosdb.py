from azure.cosmos import CosmosClient
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

load_dotenv()

url = os.getenv("COSMOS_EXPORT_URL")
key = os.getenv("COSMOS_EXPORT_KEY")
database_name = os.getenv("COSMOS_EXPORT_DB_NAME")
client = CosmosClient(url, credential=key)
db = client.get_database_client(database_name)

today = datetime.today().strftime('%Y-%m-%d-%H-%M')
export_folder = f"export/{database_name}_{today}"
os.makedirs(export_folder, exist_ok=True)

def export_container(container):
    container_name = container["id"]
    if(container_name != "event"):
        return
    container_client = db.get_container_client(container_name)
    items = list(container_client.query_items("SELECT * FROM c", enable_cross_partition_query=True))

    with open(f"{export_folder}/{container_name}_export.json", "w") as f:
        json.dump(items, f)

# Use ThreadPoolExecutor for parallel export
containers = list(db.list_containers())
with ThreadPoolExecutor() as executor:
    futures = [executor.submit(export_container, container) for container in containers]

    # Use tqdm for progress bar
    for _ in tqdm(as_completed(futures), total=len(containers), desc="Exporting containers", unit=" containers"):
        pass
