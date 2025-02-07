# app/webhook_listener.py
import os
import logging
from flask import Flask, request, jsonify
from app.config import Config
from app.utils import insert_raw_payload, transform_payload, upsert_transaction, mark_raw_as_processed

logging.basicConfig(level=logging.INFO)
app = Flask(__name__)

@app.route('/webhooks', methods=['POST'])
def webhook_listener():
    """
    Receives webhook payloads, stores the raw payload, transforms it,
    upserts the processed record, and marks the raw payload as processed.
    """
    payload = request.get_json()
    if not payload:
        return jsonify({"error": "Invalid payload"}), 400

    logging.info("Received webhook payload: %s", payload)
    raw_id = insert_raw_payload(payload)
    if raw_id is None:
        return jsonify({"error": "Failed to store raw payload"}), 500

    formatted_data = transform_payload(payload)
    if formatted_data:
        formatted_data["raw_id"] = raw_id
        upsert_transaction(formatted_data)
        mark_raw_as_processed(raw_id)
    else:
        logging.warning("Payload transformation failed; raw payload stored with id %s", raw_id)

    return jsonify({"status": "success"}), 200

def run_app():
    app.run(host='0.0.0.0', port=Config.PORT)

if __name__ == "__main__":
    run_app()
