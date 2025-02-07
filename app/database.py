# app/database.py
import os
from dotenv import load_dotenv, find_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, DateTime, Boolean, JSON, String, Numeric
from datetime import datetime

# Ensure that .env is loaded from the project root
load_dotenv(find_dotenv())

# Get the DATABASE_URL from environment
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise Exception("DATABASE_URL not set in environment. Please check your .env file.")

# Create SQLAlchemy engine and define metadata.
engine = create_engine(DATABASE_URL, echo=False)
metadata = MetaData()

# Define the raw payload table.
helius_hook = Table(
    'helius_hook', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('received_at', DateTime, default=datetime.utcnow, nullable=False),
    Column('payload', JSON, nullable=False),
    Column('processed', Boolean, default=False, nullable=False)
)

# Define the processed (formatted) transactions table.
helius_txns_clean = Table(
    'helius_txns_clean', metadata,
    Column('raw_id', Integer, nullable=True),
    Column('user_address', String(255), nullable=True),
    Column('swapfromtoken', String(255), nullable=True),
    Column('swapfromamount', Numeric, nullable=True),
    Column('swaptotoken', String(255), nullable=True),
    Column('swaptotoamount', Numeric, nullable=True),
    Column('signature', String(255), primary_key=True),
    Column('source', String(255), nullable=True),
    Column('timestamp', DateTime, nullable=True)
)

# Create tables if they do not exist.
metadata.create_all(engine)
