import os
import logging
import json
import requests
from datetime import datetime
from sqlalchemy import create_engine, text
from config import Config

def fetch_addresses_from_db():
    """
    Connect to the database and retrieve distinct addresses from the list_whales_top table,
    using the latest timestamp.
    Returns a list of addresses.
    """
    database_url = Config.DATABASE_URL  # Ensure your .env sets DATABASE_URL and Config loads it.
    engine = create_engine(database_url)
    with engine.connect() as connection:
        # Use .mappings() to get dictionary-like rows.
        result = connection.execute(text("""
            SELECT DISTINCT owner as address 
            FROM trader_filtered
        """))
        addresses = [row['address'] for row in result.mappings() if row['address']]
    return addresses

def update_helius_webhook_sdk(new_url):
    """
    Updates the Helius webhook using a direct PUT request.
    It queries the database for a list of addresses to update the "accountAddresses" field.
    
    Required environment variables:
      - HELIUS_API_KEY: Your Helius API key.
      - HELIUS_WEBHOOK_ID: The webhook ID to update.
      - HELIUS_TRANSACTION_TYPES: (Optional) Comma-separated list (default: "SWAP").
      - HELIUS_WEBHOOK_TYPE: (Optional, default: "enhanced").
      - HELIUS_TXN_STATUS: (Optional, default: "all").
      - HELIUS_AUTH_HEADER: (Optional).
    """
    api_key = os.getenv("HELIUS_API_KEY")
    if not api_key:
        logging.error("HELIUS_API_KEY is not set.")
        return None

    # Get the webhook ID (or use default)
    webhook_id = os.getenv("HELIUS_WEBHOOK_ID")

    # Prepare transaction types; default to "SWAP" if not provided.
    transaction_types_env = os.getenv("HELIUS_TRANSACTION_TYPES", "SWAP")
    transaction_types = [x.strip() for x in transaction_types_env.split(",")] if transaction_types_env else ["SWAP"]

    # Fetch addresses from the database.
    account_addresses = fetch_addresses_from_db()
    logging.debug("Fetched addresses from DB: %s", account_addresses)
    
    # Other optional parameters.
    webhook_type = os.getenv("HELIUS_WEBHOOK_TYPE", "enhanced")
    auth_header = os.getenv("HELIUS_AUTH_HEADER", "")

    # Build the API URL and payload.
    url_api = f"https://api.helius.xyz/v0/webhooks/{webhook_id}?api-key={api_key}"
    payload = {
        "webhookURL": new_url,
        "transactionTypes": transaction_types,
        "accountAddresses": account_addresses,
        "webhookType": webhook_type,
        "authHeader": auth_header
    }

    logging.debug("Sending payload to Helius: %s", payload)
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.put(url_api, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            logging.info("Successfully updated Helius webhook.")
            data = response.json()
            logging.debug("Response data: %s", data)
            return data
        else:
            logging.error("Failed to update webhook. Status code: %s, response: %s", response.status_code, response.text)
            return None
    except Exception as e:
        logging.error("Exception while updating Helius webhook: %s", e)
        return None
