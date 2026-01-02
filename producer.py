import os
import json
import time
import requests
import websocket
import boto3  # Ensure 'boto3' is in your requirements.txt

# 1. Setup AWS SQS Client
# These credentials must be set in Render's "Environment" tab
sqs = boto3.client(
    'sqs',
    region_name=os.environ.get('AWS_REGION'),
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
)
QUEUE_URL = os.environ.get('SQS_QUEUE_URL')

def get_nobil_url():
    api_key = os.environ.get('NOBIL_API_KEY') 
    headers = {
        "x-api-key": api_key,
        "Content-Length": "0" 
    }
    url_endpoint = "https://api.data.enova.no/nobil/real-time/v1/Realtime"
    
    print(f"🌐 Fetching live stream URL from: {url_endpoint}", flush=True)
    try:
        # Verified: Use .post() as shown in your Postman setup
        response = requests.post(url_endpoint, headers=headers)
        response.raise_for_status()
        
        # CORRECTED: Using 'accessToken' as per your working code
        url = response.json().get('accessToken')
        print(f"🔗 WebSocket URL Received: {url}", flush=True)
        return url
    except Exception as e:
        print(f"❌ API Error: Could not get URL. {e}", flush=True)
        return None

def on_message(ws, message):
    try:
        data = json.loads(message)
        nobil_id = data.get('nobilId', "")
        
        # --- FILTERING LOGIC ---
        if nobil_id.startswith("SWE_7"):
            print(f"⭐ MATCH FOUND! ID: {nobil_id}. Sending to SQS...", flush=True)
            
            # Send the filtered data to AWS SQS
            sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(data)
            )
        else:
            # Keep printing dots so you know it's alive
            print(".", end="", flush=True) 
            
    except Exception as e:
        print(f"\n⚠️ Error: {e}", flush=True)

def on_error(ws, error):
    print(f"\n❗ WebSocket Error: {error}", flush=True)

def on_close(ws, close_status_code, close_msg):
    print(f"\n🔌 Connection Closed. Reconnecting in 10s...", flush=True)
    time.sleep(10)
    start_streaming()

def start_streaming():
    url = get_nobil_url()
    if not url:
        print("Retry in 30s...", flush=True)
        time.sleep(30)
        start_streaming()
        return

    print("🚀 Connecting to WebSocket and AWS SQS...", flush=True)
    ws = websocket.WebSocketApp(
        url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    # Ping keeps the connection alive on Render's network
    ws.run_forever(ping_interval=30, ping_timeout=10)

if __name__ == "__main__":
    start_streaming()
