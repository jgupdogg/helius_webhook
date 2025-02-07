# app/config.py
import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    DATABASE_URL = os.getenv("DATABASE_URL")
    if not DATABASE_URL:
        raise Exception("DATABASE_URL is not set in the environment. Please check your .env file.")
    HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
    WEBHOOK_URL = os.getenv("WEBHOOK_URL")
    NGROK_TOKEN = os.getenv("NGROK_TOKEN")
    USE_NGROK = os.getenv("USE_NGROK", "false").lower() == "true"
    PORT = int(os.getenv("PORT", 5000))
    WEBHOOK_TYPE = os.getenv("WEBHOOK_TYPE", "raw")
    WEBHOOK_AUTH_HEADER = os.getenv("WEBHOOK_AUTH_HEADER")
