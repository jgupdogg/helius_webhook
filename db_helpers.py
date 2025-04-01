# db_helpers.py
import os
import json
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv, find_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO)

# Find and load the .env file
dotenv_path = find_dotenv()
logging.info(f"Found .env file at: {dotenv_path}")
load_dotenv(dotenv_path)

# Debug environment variables
logging.info(f"All environment variables: {dict(os.environ)}")
database_url = os.getenv("DATABASE_URL")
logging.info(f"DATABASE_URL from environment: {database_url}")

def fetch_addresses_from_db():
    """
    Connect to the database and retrieve distinct addresses.
    Returns a list of addresses.
    """
    # Force reload from .env file
    load_dotenv(override=True)
    database_url = os.getenv("DATABASE_URL")
    
    logging.info("Fetching addresses from DB using URL: %s", database_url)
    if not database_url:
        logging.error("DATABASE_URL is not set.")
        return []
    engine = create_engine(database_url)
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text("""
SELECT *
FROM helius_addresses
WHERE CAST(last_updated AS TIMESTAMP) >= (CURRENT_TIMESTAMP - INTERVAL '30 days')
                     """)
            ).mappings()
            addresses = [row["address"] for row in result if row["address"]]
            logging.info("Fetched addresses from DB: %s", len(addresses))
            return addresses
    except SQLAlchemyError as e:
        logging.error("Error fetching addresses: %s", e)
        return []

def insert_raw_payload(payload):
    """
    Inserts the raw JSON payload into the helius_hook table.
    Assumes the table 'helius_hook' exists with columns:
      - id (primary key)
      - payload (text)
      - received_at (timestamp)
      - processed (boolean)
    """
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logging.error("DATABASE_URL is not set.")
        return None
    engine = create_engine(database_url)
    try:
        with engine.begin() as connection:
            query = text("""
                INSERT INTO helius_hook (payload, received_at, processed)
                VALUES (:payload, :received_at, false)
                RETURNING id
            """)
            result = connection.execute(query, {
                "payload": json.dumps(payload),
                "received_at": datetime.utcnow()
            })
            inserted_id = result.fetchone()[0]
            logging.info("Inserted raw payload with id %s", inserted_id)
            return inserted_id
    except SQLAlchemyError as e:
        logging.error("Error inserting raw payload: %s", e)
        return None
