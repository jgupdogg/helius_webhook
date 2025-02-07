# app/utils.py
import json
import logging
from datetime import datetime
from sqlalchemy import text
from app.database import engine

def insert_raw_payload(payload):
    """
    Inserts the raw JSON payload into the helius_hook table.
    Returns the inserted record's id.
    """
    try:
        with engine.begin() as connection:
            query = text("""
                INSERT INTO helius_hook (payload, received_at, processed)
                VALUES (:payload, :received_at, false)
                RETURNING id
            """)
            result = connection.execute(query,
                                        payload=json.dumps(payload),
                                        received_at=datetime.utcnow())
            inserted_id = result.fetchone()[0]
            logging.info("Inserted raw payload with id %s", inserted_id)
            return inserted_id
    except Exception as e:
        logging.error("Error inserting raw payload: %s", e)
        return None

def transform_payload(payload):
    """
    Transforms the raw payload (a list of transactions) into a structured dictionary.
    """
    logging.info("Transforming payload: %s", payload)
    if not payload or not isinstance(payload, list):
        logging.warning("Payload is empty or not a list.")
        return None

    tx = payload[0]  # Process the first transaction
    token_transfers = tx.get("tokenTransfers", [])
    if not token_transfers:
        logging.warning("No tokenTransfers found in transaction.")
        return None

    first_tt = token_transfers[0]
    last_tt = token_transfers[-1]
    ts = tx.get("timestamp")
    ts_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S") if ts else None

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
    Upserts the transformed transaction into the helius_txns_clean table.
    """
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
            connection.execute(query, **formatted_data)
            logging.info("Upserted transaction with signature: %s", formatted_data.get("signature"))
    except Exception as e:
        logging.error("Error upserting transaction: %s", e)

def mark_raw_as_processed(raw_id):
    """
    Marks the raw payload as processed.
    """
    try:
        with engine.begin() as connection:
            query = text("UPDATE helius_hook SET processed = true WHERE id = :id")
            connection.execute(query, id=raw_id)
            logging.info("Marked raw payload id %s as processed.", raw_id)
    except Exception as e:
        logging.error("Error marking raw payload id %s as processed: %s", raw_id, e)
