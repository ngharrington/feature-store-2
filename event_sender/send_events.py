import datetime
import os
import time
import uuid

import requests

# URL of the user access service
url = os.environ.get("EVENT_URL", "http://localhost:5000/event")

# Simulate sending various events
events = [
    {
        "name": "scam_flag",
        "event_properties": {
            "user_id": "user123",
        },
    },
    {
        "name": "add_credit_card",
        "event_properties": {"zipcode": "12345", "user_id": "user123"},
    },
    {"name": "purchase", "event_properties": {"user_id": "user123", "amount": 50}},
    {"name": "chargeback", "event_properties": {"user_id": "user123", "amount": 30}},
]


def refresh_fields(event):
    """give unique/new values for required fields"""
    event["uuid"] = str(uuid.uuid4())
    event["timestamp"] = datetime.datetime.now().isoformat()


def send_events():
    time.sleep(1)
    while True:
        for event in events:
            refresh_fields(event)
            print(event)
            response = requests.post(url, json=event)
            print(
                f"Sent event {event['name']}, response status: {response.status_code}, {response.text}"
            )
            time.sleep(3)  # Wait a bit before sending the next event


if __name__ == "__main__":
    print("RUNNING")
    send_events()
