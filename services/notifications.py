from models.event import Event

DEFAULT_EVENT_SUBSCRIBERS_MAP = {
    "access_granted": ["https:://api.example.com/event"],
    "access_revoked": ["https:://api.example.com/event"],
}


# Assume this is a resilent service to send notifications through
# that handles deduplication, retries, and such for us.
class NotificationsService:
    def __init__(self):
        # hardcodeing for demonstration purposes
        self._event_subscribers = DEFAULT_EVENT_SUBSCRIBERS_MAP

    def send_notification(self, event: Event):
        subscribers = self._event_subscribers.get(event.name)
        if not subscribers:
            return
        for subscriber in subscribers:
            self._send_notification(subscriber, event)

    def _send_notification(self, subscriber: str, event: dict):
        print(f"Sending notification to {subscriber} for event {event}")
        return