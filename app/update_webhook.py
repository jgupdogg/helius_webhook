#!/usr/bin/env python
import os
import json
import logging
import requests
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables from .env file (if available)
load_dotenv()

# Set logging to DEBUG.
logging.basicConfig(level=logging.DEBUG)

def fetch_addresses_from_db():
    """
    Connects to the database and retrieves distinct addresses from the trader_filtered table.
    Returns a list of addresses.
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logging.error("DATABASE_URL is not set.")
        return []
    engine = create_engine(database_url)
    with engine.connect() as connection:
        # Use .mappings() to get dictionary-like rows.
        result = connection.execute(text("SELECT DISTINCT owner as address FROM trader_filtered LIMIT 2")).mappings()
        addresses = [row['address'] for row in result if row['address']]
    return addresses

def update_helius_webhook(new_url):
    """
    Updates the Helius webhook using a PUT request.
    
    The function fetches addresses from the database, joins them into a comma-separated string,
    and submits the new webhook URL along with other parameters.
    """
    api_key = os.getenv("HELIUS_API_KEY")
    webhook_id = os.getenv("HELIUS_WEBHOOK_ID")
    if not api_key or not webhook_id:
        logging.error("Missing HELIUS_API_KEY or HELIUS_WEBHOOK_ID.")
        return None

    # Fetch addresses and join them into a comma-separated string.
    addresses = fetch_addresses_from_db()
    addresses_str = ",".join(addresses)
    logging.debug("Fetched addresses: %s", addresses_str)

    # Prepare other parameters.
    transaction_types = os.getenv("HELIUS_TRANSACTION_TYPES", "SWAP").split(',')
    transaction_types = [x.strip() for x in transaction_types]
    webhook_type = os.getenv("HELIUS_WEBHOOK_TYPE", "enhanced")
    txn_status = os.getenv("HELIUS_TXN_STATUS", "all")
    auth_header = os.getenv("HELIUS_AUTH_HEADER", "")

    # Build the Helius API URL.
    url_api = f"https://api.helius.xyz/v0/webhooks/{webhook_id}?api-key={api_key}"
    payload = {
        "webhookURL": new_url,
        "transactionTypes": transaction_types,
        "accountAddresses": addresses_str,
        "webhookType": webhook_type,
        "txnStatus": txn_status,
        "authHeader": auth_header
    }
    logging.debug("Sending payload to Helius: %s", payload)
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.put(url_api, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            logging.info("Successfully updated Helius webhook.")
            return response.json()
        else:
            logging.error("Failed to update webhook. Status code: %s, response: %s",
                          response.status_code, response.text)
            return None
    except Exception as e:
        logging.error("Exception while updating Helius webhook: %s", e)
        return None

if __name__ == '__main__':
    # The new webhook URL to update; usually you’d set this to your public URL + "/webhooks".
    new_url = os.getenv("NEW_WEBHOOK_URL")
    if not new_url:
        logging.error("NEW_WEBHOOK_URL is not set.")
    else:
        update_response = update_helius_webhook(new_url)
        logging.info("Webhook update response: %s", update_response)
