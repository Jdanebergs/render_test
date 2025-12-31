import os
import json
import time
import requests
import websocket

# 1. Function to get the live Stream URL from NOBIL
def get_nobil_url():
    # Ensure this variable is named NOBIL_API_KEY in your Render environment
    api_key = os.environ.get('NOBIL_API_KEY') 
    
    # Headers must match your working Postman setup
    headers = {
        "x-api-key": api_key,
        "Content-Length": "0"  # Postman showed Content-Length: 0
    }
    
    # Use the Enova endpoint from your Postman screenshot
    url_endpoint = "https://api.data.enova.no/nobil/real-time/v1/Realtime"
    
    print(f"🌐 Fetching live stream URL from: {url_endpoint}", flush=True)
    try:
        # CRITICAL: Use .post() instead of .get()
        response = requests.post(url_endpoint, headers=headers)
        response.raise_for_status()
        
        # Extract the websocket URL from the response
        url = response.json().get('accessToken')
        print(f"🔗 WebSocket URL Received: {url}", flush=True)
        return url
    except Exception as e:
        print(f"❌ API Error: Could not get URL. {e}", flush=True)
        return None

# 2. Handle incoming data
def on_message(ws, message):
    try:
        data = json.loads(message)
        nobil_id = data.get('nobilId', "")
        
        # --- TEST FILTERING ---
        if nobil_id.startswith("SWE_7"):
            # We use bright emojis so it's easy to spot in the Render logs
            print(f"⭐ MATCH FOUND! ID: {nobil_id} | Status: {data.get('status')}", flush=True)
        else:
            # We print a dot for every skipped message to show the stream is alive 
            # without filling the screen with text.
            print(".", end="", flush=True) 
            
    except Exception as e:
        print(f"\n⚠️ Data Error: {e}", flush=True)

def on_error(ws, error):
    print(f"\n❗ WebSocket Error: {error}", flush=True)

def on_close(ws, close_status_code, close_msg):
    print(f"\n🔌 Connection Closed. Reconnecting in 10s...", flush=True)
    time.sleep(10)
    start_streaming()

# 3. Main Loop
def start_streaming():
    url = get_nobil_url()
    
    if not url:
        print("Retry in 30s...", flush=True)
        time.sleep(30)
        start_streaming()
        return

    print("🚀 Connecting to WebSocket...", flush=True)
    ws = websocket.WebSocketApp(
        url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    
    # ping_interval keeps the connection alive on Render's network
    ws.run_forever(ping_interval=30, ping_timeout=10)

if __name__ == "__main__":
    start_streaming()


"""
import os
import json
import time
import requests
import websocket
import boto3


# 1. Setup AWS SQS Client
sqs = boto3.client(
    'sqs',
    region_name=os.environ.get('AWS_REGION'),
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
)
QUEUE_URL = os.environ.get('SQS_QUEUE_URL')


# 2. Function to get the live Stream URL from NOBIL
def get_nobil_url():
    api_key = os.environ.get('NOBIL_API_KEY')
    # This endpoint may vary based on NOBIL's latest docs
    headers = {"Ocp-Apim-Subscription-Key": api_key}
    response = requests.get("https://api.nobil.no/realtime/connection", headers=headers)
    return response.json()['url'] 

# 3. Handle incoming data
def on_message(ws, message):
    try:
        # 1. Convert the raw text from NOBIL into a Python dictionary
        data = json.loads(message)
        
        # 2. Extract the nobilId (using .get() prevents crashes if the key is missing)
        nobil_id = data.get('nobilId', "")
        
        # 3. Apply your filter
        # This will catch "SWE_72886", "SWE_7999", etc.
        if nobil_id.startswith("SWE_7"):
            print(f"✅ MATCH FOUND: {nobil_id}. Sending to SQS...")
            
            # Push the data to your AWS SQS queue
            sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(data)
            )
        else:
            # This logs data that doesn't match, so you can see the filter working
            print(f"skipping: {nobil_id}")
            
    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### Connection Closed, Reconnecting in 5s... ###")
    time.sleep(5)
    start_streaming()

# 4. Main Loop
def start_streaming():
    try:
        stream_url = get_nobil_url()
        
        ws = websocket.WebSocketApp(
            stream_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever()
    
    except Exception as e:
        print(f"Failed to start: {e}. Retrying...")
        time.sleep(10)
        start_streaming()

if __name__ == "__main__":
    start_streaming()

"""
