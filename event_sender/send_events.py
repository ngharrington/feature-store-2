import requests
import time

# URL of the user access service
url = "http://user_access_service:5000/event"

# Simulate sending various events
events = [
    {"name": "scam_flag", "event_properties": { "user_id": "user123",}},
    {"name": "add_credit_card", "event_properties": {"zipcode": "12345", "user_id": "user123"}},
    {"name": "chargeback", "event_properties": { "user_id": "user123", "amount": 50, "total_spend": 500}},
]

def send_events():
    while True:
        for event in events:
            response = requests.post(url, json=event)
            print(f"Sent event {event['name']}, response status: {response.status_code}")
            time.sleep(5)  # Wait a bit before sending the next event

if __name__ == "__main__":
    send_events()
