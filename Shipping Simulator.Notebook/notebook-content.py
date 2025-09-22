# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5373acee-9fca-479c-b525-4d7e879252f0",
# META       "default_lakehouse_name": "Zava_lakehouse",
# META       "default_lakehouse_workspace_id": "0feddad4-30ec-4838-93c8-cad654a164fa",
# META       "known_lakehouses": [
# META         {
# META           "id": "ed2b117e-32c7-48f1-bcca-92941b562650"
# META         },
# META         {
# META           "id": "5373acee-9fca-479c-b525-4d7e879252f0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Cell 0: Install required library for Azure Data Lake Storage upload
!pip install azure-storage-blob

# Cell 1: Import libraries
import uuid
import random
import time
from datetime import datetime, timedelta
import threading
import xml.etree.ElementTree as ET
from azure.storage.blob import BlobServiceClient

# -- Configurable Variables --
BLOB_CONNECTION_STRING = "https://rtihackstorage.blob.core.windows.net/"
CONTAINER_NAME = "shipping-events"

SHIPPING_PROVIDERS = ["FedEx", "DPD", "UPS"]

DISTRIBUTION_CENTERS = [
    {"country": "Germany", "city": "Berlin", "lat": 52.5200, "long": 13.4050}
 ]

DESTINATIONS = [
    {"country": "Germany", "city": "Berlin", "lat": 52.5200, "long": 13.4050},
    {"country": "China", "city": "Shanghai", "lat": 31.2304, "long": 121.4737},
    {"country": "USA", "city": "Austin", "lat": 30.2672, "long": -97.7431},
    {"country": "USA", "city": "Buffalo", "lat": 42.8864, "long": -78.8784},
    {"country": "Spain", "city": "Barcelona", "lat": 41.3851, "long": 2.1734},
    {"country": "India", "city": "Mumbai", "lat": 19.0760, "long": 72.8777},
    {"country": "France", "city": "Paris", "lat": 48.8566, "long": 2.3522},
    {"country": "UK", "city": "London", "lat": 51.5074, "long": -0.1278},
    {"country": "Italy", "city": "Rome", "lat": 41.9028, "long": 12.4964},
    {"country": "Netherlands", "city": "Amsterdam", "lat": 52.3676, "long": 4.9041}
 ]

# Initialize Azure Blob Service client
blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)
try:
    container_client.create_container()
except Exception:
    pass  # Container may already exist

# --- Helper Functions ---
def random_address(destination):
    streets = ["Main St", "High St", "Elm St", "Oak St", "Maple Ave"]
    zip_code = str(random.randint(10000, 99999))
    street = random.choice(streets)
    number = str(random.randint(1, 200))
    return {
        "name": f"{random.choice(['Alice', 'Bob', 'Charlie', 'Diana', 'Eve', 'Andrew', 'John', 'Emma'])} {random.choice(['Smith', 'Johnson', 'Brown', 'Williams', 'Jones', 'Kieran'])}",
        "street": f"{street} {number}",
        "city": destination["city"],
        "zip": zip_code,
        "country": destination["country"],
        "latitude": destination["lat"],
        "longitude": destination["long"]
    }

def random_product():
    return {
        "ProductId": f"PROD{random.randint(4000, 4019)}",
        "size": random.choice(["36", "38", "40", "42", "44"]),
        "qty": random.randint(1, 3)
    }

def random_weight(item):
    return round(random.uniform(0.7, 1.2) * item["qty"], 2)

def random_provider():
    return random.choice(SHIPPING_PROVIDERS)

def random_distribution_center():
    return random.choice(DISTRIBUTION_CENTERS)

def random_destination():
    return random.choice(DESTINATIONS)

# --- Generate XML shipping event ---
def create_shipping_xml_event(order_number, src, dst, item, weight, provider, status, delay_reason, anomaly_flag, event_time):
    root = ET.Element("ShippingEvent")
    ET.SubElement(root, "Provider").text = provider
    ET.SubElement(root, "OrderNumber").text = order_number
    ET.SubElement(root, "EventTime").text = event_time
    ET.SubElement(root, "Status").text = status
    ET.SubElement(root, "DelayReason").text = delay_reason
    ET.SubElement(root, "AnomalyFlag").text = anomaly_flag

    dc = ET.SubElement(root, "SourceDistributionCenter")
    ET.SubElement(dc, "City").text = src["city"]
    ET.SubElement(dc, "Country").text = src["country"]
    ET.SubElement(dc, "Latitude").text = str(src["lat"])
    ET.SubElement(dc, "Longitude").text = str(src["long"])

    addr = ET.SubElement(root, "DestinationAddress")
    ET.SubElement(addr, "Name").text = dst["name"]
    ET.SubElement(addr, "Street").text = dst["street"]
    ET.SubElement(addr, "City").text = dst["city"]
    ET.SubElement(addr, "Zip").text = dst["zip"]
    ET.SubElement(addr, "Country").text = dst["country"]
    ET.SubElement(addr, "Latitude").text = str(dst["latitude"])
    ET.SubElement(addr, "Longitude").text = str(dst["longitude"])

    ship_info = ET.SubElement(root, "ShippingContents")
    prod = ET.SubElement(ship_info, "Product")
    ET.SubElement(prod, "ProductId").text = item["ProductId"]
    ET.SubElement(prod, "Size").text = item["size"]
    ET.SubElement(prod, "Quantity").text = str(item["qty"])

    ET.SubElement(root, "WeightKg").text = str(weight)

    return ET.tostring(root, encoding='utf-8', method='xml')

# --- Blob upload helper ---
def upload_xml_to_blob(xml_bytes, filename):
    blob_client = container_client.get_blob_client(filename)
    blob_client.upload_blob(xml_bytes, overwrite=True)

# --- Main Simulation ---
def simulate_shipping_events(rate_per_minute=6):
    interval = 60 / rate_per_minute
    order_event_status = {}  # Track status for each order_number

    try:
        while True:
            order_number = str(uuid.uuid4())
            src = random_distribution_center()
            dst_info = random_destination()
            dst = random_address(dst_info)
            provider = random_provider()
            item = random_product()
            weight = random_weight(item)

            delay_reason = random.choice(["Monsoon", "Port Closed", "None"])
            anomaly_flag = str(random.random() < 0.1).lower()

            # Store all order details for consistency
            order_event_status[order_number] = {
                "product_id": item["ProductId"],
                "provider": provider,
                "src": src,
                "dst": dst,
                "item": item,
                "weight": weight,
                "status": set()
            }

            # Dispatched
            status = "Dispatched"
            event_time = datetime.utcnow().isoformat() + "Z"
            xml_bytes = create_shipping_xml_event(order_number, src, dst, item, weight, provider, status, delay_reason, anomaly_flag, event_time)
            filename = f"{provider}_order_{order_number}_{item['ProductId']}_dispatched.xml"
            upload_xml_to_blob(xml_bytes, filename)
            print(f"Shipping event dispatched: {filename}")
            order_event_status[order_number]["status"].add("Dispatched")

            # PickedUp (some orders only)
            if random.random() < 0.7:
                status = "PickedUp"
                event_time = (datetime.utcnow() + timedelta(seconds=random.randint(10, 60))).isoformat() + "Z"
                xml_bytes_pickup = create_shipping_xml_event(order_number, src, dst, item, weight, provider, status, delay_reason, anomaly_flag, event_time)
                filename_pickup = f"{provider}_order_{order_number}_{item['ProductId']}_pickedup.xml"
                upload_xml_to_blob(xml_bytes_pickup, filename_pickup)
                print(f"Shipping event picked up: {filename_pickup}")
                order_event_status[order_number]["status"].add("PickedUp")

            # Routing (some orders only, only if PickedUp)
            if "PickedUp" in order_event_status[order_number]["status"] and random.random() < 0.5:
                status = "Routing"
                event_time = (datetime.utcnow() + timedelta(seconds=random.randint(61, 180))).isoformat() + "Z"
                xml_bytes_routing = create_shipping_xml_event(order_number, src, dst, item, weight, provider, status, delay_reason, anomaly_flag, event_time)
                filename_routing = f"{provider}_order_{order_number}_{item['ProductId']}_routing.xml"
                upload_xml_to_blob(xml_bytes_routing, filename_routing)
                print(f"Shipping event routing: {filename_routing}")
                order_event_status[order_number]["status"].add("Routing")

            time.sleep(interval)
    except KeyboardInterrupt:
        print("Simulation stopped.")

# --- Run Simulation in Background ---
thread = threading.Thread(target=simulate_shipping_events, args=(8,), daemon=True)
thread.start()

print("ZAVA shipping event simulator is running. Press Ctrl+C to stop the notebook.")

while True:
    time.sleep(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
