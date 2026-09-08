"""
PaymentGuard - Live Transaction Stream Simulator
Simulates real-time credit card transactions and sends them to Microsoft Fabric Eventstream
via the Azure Event Hubs protocol or REST endpoint.

Prerequisites:
    pip install azure-eventhub requests faker
"""

import os
import sys
import json
import time
import random
import uuid
from datetime import datetime, timezone

# Optional dependency imports with helpful fallbacks
try:
    from azure.eventhub import EventHubProducerClient, EventData
    AZURE_EVENTHUB_AVAILABLE = True
except ImportError:
    AZURE_EVENTHUB_AVAILABLE = False

try:
    from faker import Faker
    fake = Faker()
except ImportError:
    fake = None


# ==========================================
# CONFIGURATION
# ==========================================
# Replace with your Eventstream Custom App connection string:
# In Fabric: Eventstream -> Custom App source -> Keys / Connection string
EVENT_HUB_CONNECTION_STRING = os.getenv(
    "FABRIC_EVENTSTREAM_CONNECTION_STRING",
    ""  # Paste your 'Endpoint=sb://...' string here if not using env vars
)
EVENT_HUB_NAME = os.getenv("FABRIC_EVENTSTREAM_NAME", "")  # Optional: specify hub name if needed

# If you prefer direct REST POST instead of Event Hub SDK:
REST_ENDPOINT_URL = os.getenv("FABRIC_REST_ENDPOINT_URL", "")

MERCHANT_CATEGORIES = [
    "Grocery",
    "Online Retail",
    "Electronics",
    "Dining & Entertainment",
    "Travel & Airline",
    "Luxury Goods",
    "Gas & Fuel",
    "Digital Goods & Gaming"
]

CUSTOMER_SEGMENTS = ["Standard", "Premium", "Corporate", "New Account"]
CHANNELS = ["POS_CARD_PRESENT", "ONLINE_ECOMMERCE", "MOBILE_WALLET", "ATM"]

# Pre-generate a pool of 50 accounts and 30 merchants for realistic repeat behavior
SAMPLE_ACCOUNTS = [f"ACC_{1000 + i}" for i in range(50)]
SAMPLE_MERCHANTS = {
    f"M_{100 + i}": random.choice(MERCHANT_CATEGORIES) for i in range(30)
}


def generate_normal_transaction():
    """Generates a standard, realistic transaction."""
    account_id = random.choice(SAMPLE_ACCOUNTS)
    merchant_id = random.choice(list(SAMPLE_MERCHANTS.keys()))
    category = SAMPLE_MERCHANTS[merchant_id]
    
    # Category-specific realistic amounts
    if category == "Grocery":
        amount = round(random.uniform(15.0, 180.0), 2)
    elif category == "Online Retail":
        amount = round(random.uniform(20.0, 350.0), 2)
    elif category == "Electronics":
        amount = round(random.uniform(100.0, 1200.0), 2)
    elif category == "Luxury Goods":
        amount = round(random.uniform(300.0, 2500.0), 2)
    elif category == "Dining & Entertainment":
        amount = round(random.uniform(12.0, 120.0), 2)
    else:
        amount = round(random.uniform(10.0, 300.0), 2)

    risk_score = round(random.uniform(0.01, 0.25), 4)

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "customer_segment": random.choice(CUSTOMER_SEGMENTS),
        "merchant_id": merchant_id,
        "merchant_category": category,
        "amount": amount,
        "channel": random.choice(CHANNELS),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "risk_score": risk_score,
        "is_simulated_anomaly": 0,
        "anomaly_reason": "None"
    }


def generate_anomalous_transaction(anomaly_type="high_amount"):
    """
    Deliberately injects an anomaly:
    1. 'high_amount': extreme charge ($4,000 - $15,000)
    2. 'velocity_attack': rapid repeated charge burst
    3. 'high_risk_online': luxury/online purchase with high synthetic risk
    """
    account_id = random.choice(SAMPLE_ACCOUNTS)
    merchant_id = random.choice(list(SAMPLE_MERCHANTS.keys()))
    category = SAMPLE_MERCHANTS[merchant_id]

    if anomaly_type == "high_amount":
        amount = round(random.uniform(4500.0, 15000.0), 2)
        risk_score = round(random.uniform(0.75, 0.98), 4)
        reason = "Extremely high transaction amount"
    elif anomaly_type == "velocity_attack":
        amount = round(random.uniform(250.0, 950.0), 2)
        risk_score = round(random.uniform(0.60, 0.90), 4)
        reason = "Rapid consecutive card charge"
    else:
        amount = round(random.uniform(800.0, 3000.0), 2)
        risk_score = round(random.uniform(0.85, 0.99), 4)
        reason = "High-risk eCommerce profile"

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "customer_segment": random.choice(CUSTOMER_SEGMENTS),
        "merchant_id": merchant_id,
        "merchant_category": "Luxury Goods" if anomaly_type == "high_amount" else category,
        "amount": amount,
        "channel": "ONLINE_ECOMMERCE" if anomaly_type != "velocity_attack" else "POS_CARD_PRESENT",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "risk_score": risk_score,
        "is_simulated_anomaly": 1,
        "anomaly_reason": reason
    }


def send_via_eventhub(client, events):
    """Sends a batch of events using Azure Event Hubs client."""
    event_data_batch = client.create_batch()
    for ev in events:
        try:
            event_data_batch.add(EventData(json.dumps(ev)))
        except ValueError:
            # Batch is full, send and create new batch
            client.send_batch(event_data_batch)
            event_data_batch = client.create_batch()
            event_data_batch.add(EventData(json.dumps(ev)))
    client.send_batch(event_data_batch)


def run_simulator():
    print("=" * 70)
    print("PaymentGuard Live Transaction Stream Simulator")
    print("=" * 70)

    use_eventhub = False
    client = None

    if EVENT_HUB_CONNECTION_STRING and AZURE_EVENTHUB_AVAILABLE:
        try:
            print("[INFO] Connecting to Fabric Eventstream via Azure Event Hubs SDK...")
            if EVENT_HUB_NAME:
                client = EventHubProducerClient.from_connection_string(
                    conn_str=EVENT_HUB_CONNECTION_STRING,
                    eventhub_name=EVENT_HUB_NAME
                )
            else:
                client = EventHubProducerClient.from_connection_string(
                    conn_str=EVENT_HUB_CONNECTION_STRING
                )
            use_eventhub = True
            print("[SUCCESS] Connected to Eventstream producer!")
        except Exception as e:
            print(f"[WARN] Could not connect via EventHub SDK: {e}")
            print("[INFO] Falling back to DRY-RUN / local console preview mode.")
    else:
        print("[INFO] No connection string provided or azure-eventhub not installed.")
        print("[INFO] Running in CONSOLE PREVIEW mode (generates JSON to stdout).")
        print("       To send directly to Fabric:")
        print("       1. Set FABRIC_EVENTSTREAM_CONNECTION_STRING")
        print("       2. Run: pip install azure-eventhub")
        print("-" * 70)

    count = 0
    try:
        while True:
            batch = []
            # 10% chance to generate an intentional anomaly pattern
            if random.random() < 0.12:
                anomaly_type = random.choice(["high_amount", "velocity_attack", "high_risk_online"])
                if anomaly_type == "velocity_attack":
                    # Burst of 4-6 rapid transactions for the exact same account
                    target_account = random.choice(SAMPLE_ACCOUNTS)
                    burst_size = random.randint(3, 5)
                    print(f"\n[! ALERT !] Injecting VELOCITY BURST ({burst_size} txns) for {target_account}...")
                    for _ in range(burst_size):
                        txn = generate_anomalous_transaction("velocity_attack")
                        txn["account_id"] = target_account
                        batch.append(txn)
                else:
                    txn = generate_anomalous_transaction(anomaly_type)
                    print(f"\n[! ALERT !] Injecting ANOMALY ({anomaly_type}): ${txn['amount']} on {txn['account_id']}")
                    batch.append(txn)
            else:
                # Normal batch of 1-3 transactions
                for _ in range(random.randint(1, 3)):
                    batch.append(generate_normal_transaction())

            # Send or display
            if use_eventhub and client:
                send_via_eventhub(client, batch)
                print(f"[SENT] Batch of {len(batch)} txns sent to Fabric Eventstream. (Total: {count + len(batch)})")
            else:
                for txn in batch:
                    flag = "[FLAGGED]" if txn["is_simulated_anomaly"] else "[OK]     "
                    print(f"{flag} Txn: {txn['transaction_id'][:8]} | Acct: {txn['account_id']} | Cat: {txn['merchant_category']:<22} | Amt: ${txn['amount']:>8.2f} | Risk: {txn['risk_score']:.2f}")

            count += len(batch)
            # Sleep 1.5 - 3.0 seconds between batches to conserve Fabric capacity
            time.sleep(random.uniform(1.5, 3.0))

    except KeyboardInterrupt:
        print("\n" + "=" * 70)
        print(f"Simulator stopped by user. Total transactions generated: {count}")
        print("Capacity note: Eventstream ingest stops when simulator is closed.")
        print("=" * 70)
    finally:
        if client:
            client.close()


if __name__ == "__main__":
    run_simulator()
