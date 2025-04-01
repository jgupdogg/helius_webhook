# helius_api.py
import os
import logging
from helius import WebhooksAPI
from db_helpers import fetch_addresses_from_db

def update_or_create_webhook(new_url):
    """
    Uses the Helius SDK to update existing webhooks or create a new one.
    """
    api_key = os.getenv("HELIUS_API_KEY")
    if not api_key:
        logging.error("HELIUS_API_KEY is not set.")
        return

    # Initialize the Helius SDK
    webhooks_api = WebhooksAPI(api_key)
    
    # Get configuration from environment variables
    transaction_types_env = os.getenv("HELIUS_TRANSACTION_TYPES", "SWAP")
    transaction_types = [x.strip() for x in transaction_types_env.split(",")]
    webhook_type = os.getenv("HELIUS_WEBHOOK_TYPE", "enhanced")
    auth_header = os.getenv("HELIUS_AUTH_HEADER", "")
    
    # Get addresses from the database
    addresses = fetch_addresses_from_db()
    if not addresses:
        logging.error("No addresses retrieved from the DB. Exiting.")
        return
    
    try:
        # Get existing webhooks
        existing_webhooks = webhooks_api.get_all_webhooks()
        
        if existing_webhooks and len(existing_webhooks) > 0:
            # If webhooks exist, update the first one
            webhook_id = existing_webhooks[0].get("webhookID")
            if webhook_id:
                logging.info("Updating existing webhook with ID: %s", webhook_id)
                result = webhooks_api.edit_webhook(
                    webhook_id,
                    new_url,
                    transaction_types,
                    addresses,
                    webhook_type,
                    auth_header
                )
                logging.info("Webhook update result: %s", result)
            else:
                logging.error("Existing webhook found but no webhookID; creating a new webhook.")
                result = webhooks_api.create_webhook(
                    new_url,
                    transaction_types,
                    addresses,
                    webhook_type,
                    auth_header
                )
                logging.info("Webhook creation result: %s", result)
        else:
            # If no webhooks exist, create a new one
            logging.info("No existing webhook found; creating a new webhook.")
            result = webhooks_api.create_webhook(
                new_url,
                transaction_types,
                addresses,
                webhook_type,
                auth_header
            )
            logging.info("Webhook creation result: %s", result)
    except Exception as e:
        logging.error("Exception while managing webhook: %s", e)