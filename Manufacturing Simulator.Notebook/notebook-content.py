# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5373acee-9fca-479c-b525-4d7e879252f0",
# META       "default_lakehouse_name": "Zava_lakehouse",
# META       "default_lakehouse_workspace_id": "0feddad4-30ec-4838-93c8-cad654a164fa",
# META       "known_lakehouses": [
# META         {
# META           "id": "80dcda0f-d84a-4fe2-81c0-30ed84781c20"
# META         },
# META         {
# META           "id": "5373acee-9fca-479c-b525-4d7e879252f0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Cell 0: Install dependencies
!pip install azure-eventhub faker pandas

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Cell 1: Imports & Configuration
import random
import uuid
import time
from datetime import datetime, timedelta
import threading
import json
import pandas as pd
from faker import Faker
from azure.eventhub import EventHubProducerClient, EventData

# Configurable Variables
EVENT_HUB_CONNECTION_STR = "Endpoint=sb://esehamyajyf42t37kfpil3.servicebus.windows.net/;SharedAccessKeyName=key_41fd0927-4894-4d6b-ab2a-b57ef3b3fd7c;SharedAccessKey=6ovBkX3rNJdexWNQgOfDmUzFMYyjkw/qN+AEhMIyuNM=;EntityPath=es_30d097eb-8081-477f-88cf-f67332cd3e5a"
EVENT_HUB_NAME = "es_30d097eb-8081-477f-88cf-f67332cd3e5a"
EVENTS_PER_SECOND = 10

NUM_SITES = 10
NUM_ASSETS = 50
NUM_OPERATORS = 30
NUM_PRODUCTS = 20

fake = Faker()
random.seed(42)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Cell 2: Generate Site Locations
site_cities = ["Berlin", "Shanghai", "Austin", "Buffalo", "Barcelona", "Mumbai", "Paris", "London", "Rome", "Amsterdam"]

# City-to-country mapping
city_country_map = {
    "Berlin": "Germany",
    "Shanghai": "China",
    "Austin": "USA",
    "Buffalo": "USA",
    "Barcelona": "Spain",
    "Mumbai": "India",
    "Paris": "France",
    "London": "UK",
    "Rome": "Italy",
    "Amsterdam": "Netherlands"
}
site_cities = list(city_country_map.keys())

sites = []
for i in range(NUM_SITES):
    city = site_cities[i % len(site_cities)]
    country = city_country_map[city]
    sites.append({
        "SiteId": f"SITE{1000+i}",
        "City": city,
        "Country": country,
        "Latitude": round(fake.latitude(), 4),
        "Longitude": round(fake.longitude(), 4),
        "PlantType": random.choice(["Assembly", "Supplier", "Warehouse"])
    })
sites_df = pd.DataFrame(sites)
# sites_df.head(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Cell 3: Generate Assets
assets = []
for i in range(NUM_ASSETS):
    assets.append({
        "AssetId": f"ASSET{2000+i}",
        "SiteId": random.choice(sites)["SiteId"],
        "AssetName": random.choice(["Conveyor","Grinder","Printer","Assemblyline"]),
        "SerialNumber": fake.bothify(text='??##??##'),
        "MaintenanceStatus": random.choice(["Done", "Pending", "Scheduled"])
    })
assets_df = pd.DataFrame(assets)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Cell 4: Generate Operators
operators = []
for i in range(NUM_OPERATORS):
    operators.append({
        "OperatorId": f"OP{3000+i}",
        "Name": fake.name(),
        "Phone": fake.phone_number(),
        "Email": fake.email(),
        "SiteId": random.choice(sites)["SiteId"],
        "Role": random.choice(["Line Operator", "Quality Inspector", "Supervisor"]),
        "Shift": random.choice(["Day", "Night"])
    })
operators_df = pd.DataFrame(operators)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Cell 5: Generate Products
product_types = ["Cyberpunk Hat", "Oldschool Cardigan", "TropicFeel Tshirt", "CloudShell Jacket", "UrbanStep Shoes", "ClassicWear Hoodie"]
products = []
for i in range(NUM_PRODUCTS):
    products.append({
        "ProductId": f"PROD{4000+i}",
        "ProductName": random.choice(product_types),
        "SKU": f"SKU{4000+i}",
        "Brand": random.choice(["ZAVA", "AirRun", "StreetFlex", "UrbanStep", "ClassicWear"]),
        "Category": random.choice(["GenZ Pros", "Altars", "Colours", "Kids"]),
        "UnitCost": round(random.uniform(19.99, 299.99), 2)
    })
products_df = pd.DataFrame(products)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Cell 6: Event Generator Functions
def generate_production_event():
    site = random.choice(sites)
    asset = random.choice(assets)
    operator = random.choice(operators)
    product = random.choice(products)
    batch_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().isoformat() + "Z"
    defect_probability = round(random.uniform(0, 0.2), 3)
    vibration = round(random.uniform(0.5, 1.5), 2)
    temperature = round(random.uniform(20, 30), 1)
    humidity = random.randint(30, 80)
    return {
        "event_type": "production",
        "timestamp": timestamp,
        "SiteId": site["SiteId"],
        "City": site["City"],
        "AssetId": asset["AssetId"],
        "OperatorId": operator["OperatorId"],
        "OperatorName": operator["Name"],
        "ProductId": product["ProductId"],
        "SKU": product["SKU"],
        "BatchId": batch_id,
        "DefectProbability": defect_probability,
        "Vibration": vibration,
        "Temperature": temperature,
        "Humidity": humidity
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Cell 7: EventHub Producer Setup
producer = EventHubProducerClient.from_connection_string(conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Cell 8: Streaming Simulation
def emit_event(event):
    message = json.dumps(event)
    event_data = EventData(message)
    try:
        with producer:
            producer.send_batch([event_data])
    except Exception as e:
        print(f"Failed to send event: {e}")
    print(message)

def start_simulation(events_per_second=EVENTS_PER_SECOND):
    interval = 1.0  # Send a batch every second
    try:
        while True:
            batch = []
            for _ in range(events_per_second):
                event = generate_production_event()
                message = json.dumps(event)
                batch.append(EventData(message))
                print(message)
            with producer:
                producer.send_batch(batch)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Simulation stopped.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Cell 9: Run Simulation (Background Thread)
thread = threading.Thread(target=start_simulation, args=(EVENTS_PER_SECOND,), daemon=True)
thread.start()
print("Manufacturing data streaming simulator running. Press Ctrl+C to stop.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# Cell 10: Load dimension data into Lakehouse Files
# Define the target path in your Lakehouse (adjust this to your environment)
lakehouse_path = "/lakehouse/default/Files"


# Write the DataFrame to the Lakehouse in CSV format
assets_df.to_csv(lakehouse_path+"/assets.csv", index=False)
sites_df.to_csv(lakehouse_path+"/sites.csv", index=False)
products_df.to_csv(lakehouse_path+"/products.csv", index=False)
operators_df.to_csv(lakehouse_path+"/operators.csv", index=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
