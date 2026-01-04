import os
import time
import json
import argparse
import requests
from dotenv import load_dotenv

load_dotenv()

CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083")
NAME = os.getenv("CONNECTOR_NAME", "postgres-banking-connector")

def wait_for_connect(timeout_s=60):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = requests.get(f"{CONNECT_URL}/connectors", timeout=5)
            if r.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError(f"Kafka Connect not reachable at {CONNECT_URL}")

def get_connectors():
    r = requests.get(f"{CONNECT_URL}/connectors", timeout=10)
    r.raise_for_status()
    return r.json()

def delete_connector_if_exists():
    connectors = get_connectors()
    if NAME not in connectors:
        print(f"ℹ️ Connector '{NAME}' does not exist (nothing to delete).")
        return
    r = requests.delete(f"{CONNECT_URL}/connectors/{NAME}", timeout=10)
    if r.status_code in (204, 200):
        print(f"🗑️ Deleted connector '{NAME}' ✅")
    else:
        print(f"❌ Failed to delete ({r.status_code}): {r.text}")
        r.raise_for_status()

def put_config(config: dict):
    # PUT /config is idempotent: creates or updates config
    r = requests.put(
        f"{CONNECT_URL}/connectors/{NAME}/config",
        headers={"Content-Type": "application/json"},
        data=json.dumps(config),
        timeout=20,
    )
    if r.status_code not in (200, 201):
        print(f"❌ Failed to create/update connector ({r.status_code}): {r.text}")
        r.raise_for_status()
    print(f"✅ Connector '{NAME}' configured ✅")

def get_status():
    r = requests.get(f"{CONNECT_URL}/connectors/{NAME}/status", timeout=10)
    if r.status_code == 404:
        print(f"❌ No status found for connector '{NAME}' (not created).")
        return None
    r.raise_for_status()
    return r.json()

def print_status(status_json: dict):
    connector_state = status_json.get("connector", {}).get("state")
    tasks = status_json.get("tasks", [])
    task_states = [(t.get("id"), t.get("state")) for t in tasks]

    print(f"📌 Connector state: {connector_state}")
    print(f"📌 Task states: {task_states}")

    # If failed, show trace (if available)
    if connector_state == "FAILED":
        print("❌ Connector FAILED")
        print(json.dumps(status_json, indent=2))
    for t in tasks:
        if t.get("state") == "FAILED":
            print("❌ Task FAILED details:")
            print(json.dumps(t, indent=2))

def build_config_from_env():
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": os.getenv("DB_HOSTNAME", "postgres"),
        "database.port": os.getenv("DB_PORT", "5432"),
        "database.user": os.getenv("DB_USER", "postgres"),
        "database.password": os.getenv("DB_PASSWORD", "postgres"),
        "database.dbname": os.getenv("DB_NAME", "banking"),

        "topic.prefix": os.getenv("TOPIC_PREFIX", "banking_server"),
        "table.include.list": os.getenv(
            "TABLE_INCLUDE_LIST",
            "public.customers,public.accounts,public.transactions"
        ),

        "plugin.name": os.getenv("PLUGIN_NAME", "pgoutput"),
        "publication.autocreate.mode": os.getenv("PUBLICATION_AUTOCREATE_MODE", "filtered"),
        "tombstones.on.delete": os.getenv("TOMBSTONES_ON_DELETE", "false"),
        "decimal.handling.mode": os.getenv("DECIMAL_HANDLING_MODE", "double"),
        "snapshot.mode": os.getenv("SNAPSHOT_MODE", "initial"),

        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": os.getenv("KEY_SCHEMAS_ENABLE", "false"),
        "value.converter.schemas.enable": os.getenv("VALUE_SCHEMAS_ENABLE", "false"),
    }

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--recreate", action="store_true", help="Delete connector then create again")
    p.add_argument("--wait-status", type=int, default=15, help="Seconds to wait before printing status")
    args = p.parse_args()

    wait_for_connect()

    if args.recreate:
        delete_connector_if_exists()

    cfg = build_config_from_env()
    put_config(cfg)

    time.sleep(args.wait_status)
    status = get_status()
    if status:
        print_status(status)

if __name__ == "__main__":
    main()
