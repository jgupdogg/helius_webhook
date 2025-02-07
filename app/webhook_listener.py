import os
import logging
from flask import Flask, request, jsonify
from pyngrok import ngrok, conf
from config import Config
from utils import (
    update_helius_webhook_sdk
)

# Set logging to DEBUG so we can see detailed output.
logging.basicConfig(level=logging.DEBUG)
app = Flask(__name__)

# --- Your Database & Payload Functions (unchanged from your original working code) ---

def insert_raw_payload(payload):
    """
    Insert the raw JSON payload into the helius_hook table and return the inserted ID.
    """
    from sqlalchemy.exc import SQLAlchemyError
    from datetime import datetime
    import json
    from config import Config
    from sqlalchemy import create_engine, text
    # Create engine using the database URL from Config.
    engine = create_engine(Config.DATABASE_URL, echo=False)
    try:
        with engine.begin() as connection:
            query = text("""
                INSERT INTO helius_hook (payload, received_at, processed)
                VALUES (:payload, :received_at, false)
                RETURNING id
            """)
            # Pass parameters as a dictionary.
            result = connection.execute(query, {"payload": json.dumps(payload), "received_at": datetime.utcnow()})
            inserted_id = result.fetchone()[0]
            logging.info("Inserted raw payload with id %s", inserted_id)
            return inserted_id
    except SQLAlchemyError as e:
        logging.error("Error inserting raw payload: %s", e)
        return None

def transform_payload(payload):
    """
    Transform the raw payload (assumed to be a list of transactions) into a dictionary with expected keys.
    """
    if not payload or not isinstance(payload, list):
        logging.warning("Payload is empty or not a list.")
        return None

    tx = payload[0]  # Process the first transaction.
    token_transfers = tx.get("tokenTransfers", [])
    if not token_transfers:
        logging.warning("No tokenTransfers found in transaction.")
        return None

    first_tt = token_transfers[0]
    last_tt = token_transfers[-1]
    ts = tx.get("timestamp")
    ts_str = None
    if ts:
        from datetime import datetime
        ts_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    formatted = {
        "user_address": first_tt.get("fromUserAccount"),
        "swapfromtoken": first_tt.get("mint"),
        "swapfromamount": first_tt.get("tokenAmount"),
        "swaptotoken": last_tt.get("mint"),
        "swaptotoamount": last_tt.get("tokenAmount"),
        "signature": tx.get("signature"),
        "source": tx.get("source"),
        "timestamp": ts_str
    }
    return formatted

def upsert_transaction(formatted_data):
    """
    Upsert the transformed transaction into the helius_txns_clean table using the signature as the unique key.
    """
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy import create_engine, text
    from config import Config
    engine = create_engine(Config.DATABASE_URL, echo=False)
    try:
        with engine.begin() as connection:
            query = text("""
                INSERT INTO helius_txns_clean 
                    (raw_id, user_address, swapfromtoken, swapfromamount, swaptotoken, swaptotoamount, signature, source, timestamp)
                VALUES (:raw_id, :user_address, :swapfromtoken, :swapfromamount, :swaptotoken, :swaptotoamount, :signature, :source, :timestamp)
                ON CONFLICT (signature) DO UPDATE
                SET raw_id = EXCLUDED.raw_id,
                    user_address = EXCLUDED.user_address,
                    swapfromtoken = EXCLUDED.swapfromtoken,
                    swapfromamount = EXCLUDED.swapfromamount,
                    swaptotoken = EXCLUDED.swaptotoken,
                    swaptotoamount = EXCLUDED.swaptotoamount,
                    source = EXCLUDED.source,
                    timestamp = EXCLUDED.timestamp;
            """)
            connection.execute(query, formatted_data)
            logging.info("Upserted processed transaction with signature: %s", formatted_data.get("signature"))
    except SQLAlchemyError as e:
        logging.error("Error upserting transaction: %s", e)

def mark_raw_as_processed(raw_id):
    """
    Mark the raw payload as processed.
    """
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy import create_engine, text
    from config import Config
    engine = create_engine(Config.DATABASE_URL, echo=False)
    try:
        with engine.begin() as connection:
            query = text("UPDATE helius_hook SET processed = true WHERE id = :id")
            connection.execute(query, {"id": raw_id})
    except SQLAlchemyError as e:
        logging.error("Error marking raw payload id %s as processed: %s", raw_id, e)

@app.route('/webhooks', methods=['POST'])
def webhook_listener():
    """
    Endpoint called by Helius:
      1. Logs the full payload.
      2. Inserts the raw payload.
      3. Transforms the payload.
      4. Upserts the transformed record.
      5. Marks the raw payload as processed.
    """
    payload = request.get_json()
    if payload is None:
        return jsonify({"error": "Invalid payload"}), 400

    logging.info("Received webhook payload")
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

# --- Ngrok and Helius Webhook Update ---

def setup_ngrok():
    """
    Opens an ngrok tunnel on the specified port and returns the public URL.
    """
    try:
        from pyngrok import ngrok, conf
        conf.get_default().log_event_callback = lambda log: logging.debug("ngrok log: %s", log)
        port = os.getenv("PORT", "5000")
        # Using bind_tls=True ensures an HTTPS tunnel.
        ngrok_tunnel = ngrok.connect(port, bind_tls=True)
        public_url = ngrok_tunnel.public_url
        logging.debug("ngrok tunnel created: %s", ngrok_tunnel)
        logging.info("Public URL from ngrok: %s", public_url)
        return public_url
    except Exception as e:
        logging.error("Error setting up ngrok tunnel: %s", e)
        return None

if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()  # Load environment variables from .env
    logging.basicConfig(level=logging.DEBUG)
    
    # If using ngrok, update the webhook URL on Helius.
    if os.getenv("USE_NGROK", "false").lower() == "true":
        public_url = setup_ngrok()
        if public_url:
            new_webhook_url = public_url + "/webhooks"
            logging.info("New webhook URL to be set on Helius: %s", new_webhook_url)
            os.environ["WEBHOOK_URL"] = new_webhook_url

            # Update the Helius webhook using our function.
            update_response = update_helius_webhook_sdk(new_webhook_url)
            logging.info("Helius webhook update response: %s", update_response)
    else:
        logging.info("USE_NGROK not enabled. Using WEBHOOK_URL from environment: %s", os.getenv("WEBHOOK_URL"))
    
    port = int(os.getenv("PORT", "5000"))
    app.run(host='0.0.0.0', port=port)
