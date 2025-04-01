#!/usr/bin/env python
# app.py
import os
import json
import logging
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Load configuration from .env (if present)
load_dotenv()

logging.basicConfig(level=logging.DEBUG)
app = Flask(__name__)

from db_helpers import insert_raw_payload
from dag_trigger import maybe_trigger_dag
from helius_api import update_or_create_webhook

@app.route('/webhooks', methods=['POST'])
def webhook_listener():
    payload = request.get_json()
    raw_id = insert_raw_payload(payload)
    if raw_id is None:
        return jsonify({"error": "Failed to insert raw payload"}), 500
    # Trigger the Airflow DAG (with rate limiting: max once per minute)
    maybe_trigger_dag({"raw_id": raw_id})
    return jsonify({"status": "success", "raw_id": raw_id}), 200

def main():
    new_url = os.getenv("NEW_WEBHOOK_URL")
    if not new_url:
        logging.error("NEW_WEBHOOK_URL is not set. Ensure your ngrok script exports it.")
        return
    logging.info("Using NEW_WEBHOOK_URL: %s", new_url)
    update_or_create_webhook(new_url)
    logging.info("Starting Flask listener on port %s", os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")))

if __name__ == "__main__":
    main()
