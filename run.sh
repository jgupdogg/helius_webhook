#!/bin/bash
# run.sh - Script to run the webhook listener in the background

# Load environment variables from .env if it exists
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Optional: If using ngrok, set up the tunnel
if [ "$USE_NGROK" = "true" ]; then
    echo "Ngrok integration enabled..."
    # This snippet initializes ngrok and sets the WEBHOOK_URL environment variable dynamically.
    python -c "from app.config import Config; from pyngrok import ngrok; import os; \
ngrok.set_auth_token(os.getenv('NGROK_TOKEN')); \
public_url = ngrok.connect(Config.PORT).public_url; \
print('Ngrok tunnel established at ' + public_url); \
os.environ['WEBHOOK_URL'] = public_url + '/webhooks'" 
fi

nohup python -m flask run --host=0.0.0.0 --port=${PORT:-5000} > webhook.log 2>&1 &
echo "Webhook listener is running in the background. Check webhook.log for output."
