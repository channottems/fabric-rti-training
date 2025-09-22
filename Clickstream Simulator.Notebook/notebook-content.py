# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f87afd01-62ed-480d-869a-99fe3b11f421",
# META       "default_lakehouse_name": "Manufacturing",
# META       "default_lakehouse_workspace_id": "21733624-95b6-4740-b892-d29ff7889aa4",
# META       "known_lakehouses": [
# META         {
# META           "id": "f87afd01-62ed-480d-869a-99fe3b11f421"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# - spike_flag: Easily filter/aggregate viral traffic events.
# - product_id, sku: Enables product-level analytics and joins with other domains.
# - cart_spike_magnitude: Quantifies demand surges for anomaly detection and dashboards.

# CELL ********************

# Welcome to your new notebook
# Clickstream Event Simulator Notebook

import json
import uuid
import random
import time
from datetime import datetime
import threading


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

!pip install azure-eventhub

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from azure.eventhub import EventHubProducerClient, EventData

# -- Configurable Variables --
EVENT_HUB_CONNECTION_STR = "Endpoint=sb://esehamu5dm533tbsh7gx1g.servicebus.windows.net/;SharedAccessKeyName=key_f041e98c-f3a6-4734-9846-8c9915fcc505;SharedAccessKey=1ibII900Kxec7Wr04Ei1n6V8wxsjNl3nL+AEhGto+DY=;EntityPath=es_41dd9f7d-973a-4311-8c4b-6720b71520bf"
EVENT_HUB_NAME = "es_41dd9f7d-973a-4311-8c4b-6720b71520bf"
EVENTS_PER_SECOND = 10  # Adjust simulation load

USER_COUNTRIES = [
    ("DE", "Germany"), ("UK", "United Kingdom"), ("US", "United States"),
    ("FR", "France"), ("IT", "Italy"), ("ES", "Spain"),
    ("NL", "Netherlands"), ("IN", "India"), ("CN", "China")
]

PAGES = ["/", "/genz-pros", "/altars", "/colours", "/kids", "/cart", "/checkout"]
PRODUCT_IDS = [f"PROD{4000 + i}" for i in range(20)]
SKU = [f"SKU{4000 + i}" for i in range(20)]

EVENT_TYPES = [
    "page_view", "product_click", "add_to_cart", "remove_from_cart",
    "checkout_initiated", "purchase_completed", "account_created",
    "newsletter_subscribed", "newsletter_unsubscribed"
]

REFERRAL_TYPES = [
    "organic_search", "facebook", "instagram", "tiktok", "pinterest", "twitter", "direct", "other_social", "affiliate"
]



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Helper Functions ---
def random_ip():
    return ".".join(str(random.randint(1, 254)) for _ in range(4))

def now_utc():
    return datetime.utcnow().isoformat() + "Z"

def random_click_path():
    start_pages = ["/", "/genz-pros", "/altars", "/colours", "/kids"]
    num_steps = random.randint(2, 6)
    path = [random.choice(start_pages)]
    for _ in range(num_steps - 1):
        path.append(random.choice(PAGES))
    return path

def random_referral():
    ref = random.choice(REFERRAL_TYPES)
    if ref == "organic_search":
        return {"source_type": "search", "platform": "Google"}
    elif ref in {"facebook", "instagram", "tiktok", "pinterest", "twitter", "other_social"}:
        return {"source_type": "social", "platform": ref.capitalize()}
    elif ref == "affiliate":
        return {"source_type": "affiliate", "platform": "AffiliateNetwork"}
    else:  # direct
        return {"source_type": "direct", "platform": ""}

def random_browser():
    browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera", "Brave", "IE"]
    return random.choice(browsers)

def random_os():
    os_list = ["Windows 10", "Windows 11", "macOS 13", "Linux (Ubuntu)", "iOS 17", "Android 14"]
    return random.choice(os_list)

def random_device():
    devices = ["Desktop", "Laptop", "Tablet", "Mobile"]
    return random.choice(devices)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Create Producer Client ---
producer = EventHubProducerClient.from_connection_string(
    conn_str=EVENT_HUB_CONNECTION_STR,
    eventhub_name=EVENT_HUB_NAME
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Event Generator ---
def generate_event():
    country_code, country = random.choice(USER_COUNTRIES)
    event_type = random.choice(EVENT_TYPES)
    timestamp = now_utc()
    user_id = str(uuid.uuid4())
    session_id = str(uuid.uuid4())
    product_id = random.choice(PRODUCT_IDS)
    spike_flag = random.random() < 0.05  # 5% of events are spikes
    sku = random.choice(SKU)
    cart_spike_magnitude = random.randint(1, 100) if spike_flag else 0


    client_info = {
        "ip_address": random_ip(),
        "browser": random_browser(),
        "os": random_os(),
        "device": random_device()
    }

    click_path = random_click_path()
    referral = random_referral()

    payload = {}

    # Standard event payload enrichment
    if event_type == "page_view":
        payload["page"] = random.choice(PAGES)
    elif event_type in ("product_click", "add_to_cart", "remove_from_cart"):
        payload["product_id"] = random.choice(PRODUCT_IDS)
        payload["price_eur"] = round(random.uniform(49.99, 199.99), 2)
    elif event_type in ("checkout_initiated", "purchase_completed"):
        payload["cart_items"] = random.randint(1, 5)
        payload["total_value_eur"] = round(random.uniform(59.99, 499.99), 2)
    elif event_type == "account_created":
        payload["account_type"] = random.choice(["guest", "registered"])
    elif event_type in ("newsletter_subscribed", "newsletter_unsubscribed"):
        payload["newsletter"] = "ZAVA Deals"

    event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": timestamp,
        "event_type": event_type,
        "user_id": user_id,
        "session_id": session_id,
        "sku": sku,
        "country": country,
        "country_code": country_code,
        "referral_source_type": referral["source_type"],
        "referral_platform": referral["platform"],
        "product_id": product_id
    }
    return event


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# --- Event Emission ---
def emit_event(event):
    message = json.dumps(event)
    try:
        event_data = EventData(message)
        with producer:
            producer.send_batch([event_data])
    except Exception as e:
        print("Failed to send event to Event Hub:", str(e))
    print(message)

# --- Continuous Simulation ---
def start_simulation(rate_per_second=EVENTS_PER_SECOND):
    """Emit clickstream events continuously at rate_per_second."""
    interval = 1.0 / rate_per_second
    try:
        while True:
            evt = generate_event()
            emit_event(evt)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Simulation stopped.")

# --- Run Simulation in Background ---
thread = threading.Thread(target=start_simulation, args=(EVENTS_PER_SECOND,), daemon=True)
thread.start()

print("ZAVA clickstream simulator is running. Press Ctrl+C to stop the notebook.")

while True:
    time.sleep(1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
