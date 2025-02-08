#!/bin/bash
# run_all.sh
# Make sure this file is executable: chmod +x run_all.sh

# Check if ngrok is running on port 4040; if not, start it for port 5000.
if ! lsof -i:4040 >/dev/null 2>&1; then
    echo "Ngrok is not running; starting ngrok on port 5000..."
    ngrok http 5000 > /dev/null 2>&1 &
    sleep 5  # Wait a few seconds for ngrok to initialize
fi

# Retrieve the public URL from ngrok's local API using jq.
NGROK_URL=$(curl --silent http://127.0.0.1:4040/api/tunnels | jq -r '.tunnels[0].public_url')
if [ -z "$NGROK_URL" ]; then
    echo "Failed to retrieve ngrok public URL."
    exit 1
fi

# Append the webhook endpoint path.
export NEW_WEBHOOK_URL="${NGROK_URL}/webhooks"
echo "New webhook URL is: ${NEW_WEBHOOK_URL}"

# Run the Python app (which updates the webhook and then starts the Flask listener)
python app.py
