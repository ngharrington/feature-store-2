import datetime
import random
import string
import uuid

from locust import FastHttpUser, between, task


class HelloWorldUser(FastHttpUser):
    wait_time = between(0.8, 1.2)

    @task
    def hello_world(self):
        random_digits = "".join(random.choices(string.digits, k=10))
        user_id = f"user{random_digits}"
        user_id = "user123"
        payload = {
            "uuid": str(uuid.uuid4()),
            "name": "scam_flag",
            "timestamp": datetime.datetime.now().isoformat(),
            "event_properties": {
                "user_id": user_id,
            },
        }
        self.client.post("/event", json=payload)
