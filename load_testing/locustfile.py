import datetime
import random
import string
import uuid

from locust import FastHttpUser, between, task


class User(FastHttpUser):
    wait_time = between(0.8, 1.2)

    def on_start(self):
        self.user_id = self._get_random_user_id()

    def _get_random_user_id(self):
        random_digits = "".join(random.choices(string.digits, k=10))
        return f"user{random_digits}"

    @task(1)
    def send_event(self):
        payload = {
            "uuid": str(uuid.uuid4()),
            "name": "scam_flag",
            "timestamp": datetime.datetime.now().isoformat(),
            "event_properties": {
                "user_id": self.user_id,
            },
        }
        self.client.post("/event", json=payload)

    @task(1)
    def get_permission(self):
        user_id = self.user_id if random.random() < 0.25 else self._get_random_user_id()
        self.client.get("/canpurchase", headers={"x-user-id": user_id})
