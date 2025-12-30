import os
import json
import time
import requests
import websocket
import boto3

"""
# 1. Setup AWS SQS Client
sqs = boto3.client(
    'sqs',
    region_name=os.environ.get('AWS_REGION'),
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY')
)
QUEUE_URL = os.environ.get('SQS_QUEUE_URL')
"""

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
        
        """
        ws = websocket.WebSocketApp(
            stream_url,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever()
        """
    except Exception as e:
        print(f"Failed to start: {e}. Retrying...")
        time.sleep(10)
        start_streaming()

if __name__ == "__main__":
    start_streaming()
