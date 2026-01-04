import os
import json
import time
import csv
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

# 2. Load Target Stations using evse_uid
def load_target_stations(file_name):
    target_ids = set()
    try:
        # Check if file exists first
        if not os.path.exists(file_name):
            print(f"❌ ERROR: File {file_name} not found in current directory!", flush=True)
            return target_ids

        with open(file_name, mode='r', encoding='utf-8-sig') as f:
            # Diagnostic: Print the raw first line to check the delimiter and headers
            raw_header_line = f.readline()
            print(f"🔍 RAW HEADER LINE: {raw_header_line.strip()}", flush=True)
            f.seek(0) # Reset to start of file for DictReader

            reader = csv.DictReader(f)
            
            # Diagnostic: Print detected headers
            print(f"📋 DETECTED HEADERS: {reader.fieldnames}", flush=True)
            
            # Clean headers (removes extra spaces)
            if reader.fieldnames:
                reader.fieldnames = [name.strip() for name in reader.fieldnames]
            
            for row in reader:
                # IMPORTANT: This key must match one of the 'DETECTED HEADERS' exactly
                uid = row.get('evse_uid')
                if uid:
                    target_ids.add(uid.strip())
                elif not target_ids:
                    # Diagnostic: If first row fails, print it to see why
                    print(f"⚠️ FAILED TO EXTRACT ID FROM ROW: {row}", flush=True)

        print(f"✅ FINAL COUNT: Loaded {len(target_ids)} target evse_uids", flush=True)
    except Exception as e:
        print(f"⚠️ Error loading CSV: {e}", flush=True)
    return target_ids

# Initialize target set from your uploaded list
TARGET_EVSE_IDS = load_target_stations('list_of_stations.csv')

def get_nobil_url():
    api_key = os.environ.get('NOBIL_API_KEY') 
    headers = {"x-api-key": api_key, "Content-Length": "0"}
    url_endpoint = "https://api.data.enova.no/nobil/real-time/v1/Realtime"
    
    try:
        response = requests.post(url_endpoint, headers=headers)
        response.raise_for_status()
        url = response.json().get('accessToken')
        return url
    except Exception as e:
        print(f"❌ API Error: {e}", flush=True)
        return None

def on_message(ws, message):
    try:
        data = json.loads(message)
        # Extract evse_uid from the WebSocket message body
        current_evse_uid = data.get('evseUId', "")
        
        # --- MATCHING LOGIC ---
        if current_evse_uid in TARGET_EVSE_IDS:
            print(f"⭐ TARGET MATCH: {current_evse_uid}. Sending to SQS...", flush=True)
            sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(data)
            )
        else:
            # Print a dot for non-matching stations to keep logs quiet but active
            print(".", end="", flush=True)
            
    except Exception as e:
        print(f"\n⚠️ Data Error: {e}", flush=True)

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
