#!/usr/bin/env python
import os
import json
import logging
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# Load configuration from .env (if present)
load_dotenv()

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
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT DISTINCT owner as address FROM trader_filtered LIMIT 2")).mappings()
            addresses = [row["address"] for row in result if row["address"]]
            logging.info("Fetched addresses from DB: %s", addresses)
            return addresses
    except SQLAlchemyError as e:
        logging.error("Error fetching addresses: %s", e)
        return []

def get_existing_webhooks():
    """
    Retrieves all existing webhooks using the Helius API.
    Returns a list of webhook objects.
    """
    api_key = os.getenv("HELIUS_API_KEY")
    if not api_key:
        logging.error("HELIUS_API_KEY is not set.")
        return []
    url = f"https://api.helius.xyz/v0/webhooks?api-key={api_key}"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            webhooks = response.json()
            logging.info("Retrieved existing webhooks: %s", webhooks)
            return webhooks
        else:
            logging.error("Failed to get webhooks. Status code: %s, response: %s", response.status_code, response.text)
            return []
    except Exception as e:
        logging.error("Exception while getting webhooks: %s", e)
        return []

def create_webhook(new_url, addresses):
    """
    Creates a new Helius webhook using the POST API.
    """
    api_key = os.getenv("HELIUS_API_KEY")
    if not api_key:
        logging.error("HELIUS_API_KEY is not set.")
        return None
    url = f"https://api.helius.xyz/v0/webhooks?api-key={api_key}"
    
    transaction_types_env = os.getenv("HELIUS_TRANSACTION_TYPES", "SWAP")
    transaction_types = [x.strip() for x in transaction_types_env.split(",")]
    webhook_type = os.getenv("HELIUS_WEBHOOK_TYPE", "enhanced")
    txn_status = os.getenv("HELIUS_TXN_STATUS", "all")
    auth_header = os.getenv("HELIUS_AUTH_HEADER", "")

    payload = {
        "webhookURL": new_url,
        "transactionTypes": transaction_types,
        "accountAddresses": addresses,  # Send as an array
        "webhookType": webhook_type,
        "txnStatus": txn_status,
        "authHeader": auth_header
    }
    headers = {"Content-Type": "application/json"}
    logging.debug("Creating webhook with payload: %s", payload)
    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            logging.info("Successfully created webhook.")
            return response.json()
        else:
            logging.error("Failed to create webhook. Status code: %s, response: %s",
                          response.status_code, response.text)
            return None
    except Exception as e:
        logging.error("Exception while creating webhook: %s", e)
        return None

def update_webhook(webhook_id, new_url, addresses):
    """
    Updates an existing Helius webhook using the PUT API.
    """
    api_key = os.getenv("HELIUS_API_KEY")
    if not api_key:
        logging.error("HELIUS_API_KEY is not set.")
        return None
    url = f"https://api.helius.xyz/v0/webhooks/{webhook_id}?api-key={api_key}"
    
    transaction_types_env = os.getenv("HELIUS_TRANSACTION_TYPES", "SWAP")
    transaction_types = [x.strip() for x in transaction_types_env.split(",")]
    webhook_type = os.getenv("HELIUS_WEBHOOK_TYPE", "enhanced")
    txn_status = os.getenv("HELIUS_TXN_STATUS", "all")
    auth_header = os.getenv("HELIUS_AUTH_HEADER", "")

    payload = {
        "webhookURL": new_url,
        "transactionTypes": transaction_types,
        "accountAddresses": addresses,  # Send as an array
        "webhookType": webhook_type,
        "txnStatus": txn_status,
        "authHeader": auth_header
    }
    headers = {"Content-Type": "application/json"}
    logging.debug("Updating webhook with payload: %s", payload)
    try:
        response = requests.put(url, headers=headers, data=json.dumps(payload))
        if response.status_code == 200:
            logging.info("Successfully updated webhook.")
            return response.json()
        else:
            logging.error("Failed to update webhook. Status code: %s, response: %s",
                          response.status_code, response.text)
            return None
    except Exception as e:
        logging.error("Exception while updating webhook: %s", e)
        return None

def main():
    # Get the new webhook URL from the environment variable (set by run_all.sh)
    new_url = os.getenv("NEW_WEBHOOK_URL")
    if not new_url:
        logging.error("NEW_WEBHOOK_URL is not set. Please export it (e.g., via run_all.sh).")
        return

    addresses = fetch_addresses_from_db()
    if not addresses:
        logging.error("No addresses retrieved from the DB. Exiting.")
        return

    existing_webhooks = get_existing_webhooks()
    if existing_webhooks:
        # If at least one webhook exists, update the first one.
        webhook_id = existing_webhooks[0].get("webhookID")
        if webhook_id:
            logging.info("Updating existing webhook with ID: %s", webhook_id)
            result = update_webhook(webhook_id, new_url, addresses)
            logging.info("Webhook update result: %s", result)
        else:
            logging.error("No webhookID found in the first webhook entry. Cannot update.")
    else:
        # No existing webhook; create a new one.
        logging.info("No existing webhook found. Creating a new webhook.")
        result = create_webhook(new_url, addresses)
        logging.info("Webhook creation result: %s", result)

if __name__ == "__main__":
    main()
