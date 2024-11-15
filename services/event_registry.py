import asyncio
from typing import Type

from models import event


class EventTypeNotRegistered(Exception):
    pass


class EventAlreadyRegistered(Exception):
    pass


class EventSchemaRegistry:
    def __init__(self):
        self.event_schemas = {}
        self._lock = asyncio.Lock()

    async def get_schema_by_name(self, event_name: str) -> Type[event.Event]:
        async with self._lock:
            if event_name not in self.event_schemas:
                raise EventTypeNotRegistered(f"Event {event_name} not registered")
            return self.event_schemas[event_name]

    def register_event_properties_schema(
        self, event_name: str, event_schema: Type[event.Event]
    ):
        # this is read-only after initialization of the app so no need for a lock
        if event_name in self.event_schemas:
            raise EventAlreadyRegistered(f"Event {event_name} already registered")
        self.event_schemas[event_name] = event_schema
