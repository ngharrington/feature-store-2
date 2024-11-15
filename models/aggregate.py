import asyncio
import enum
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List

from pydantic import BaseModel

from models.event import Event


class AggregationError(Exception):
    pass


class AggregateType(enum.Enum):
    COUNT = "count"
    DISTINCT_COUNT = "distinct_count"
    SUM = "sum"


@dataclass
class EventAggregateConfig:
    type: AggregateType
    name: str
    event_name: str
    field: str = None

    def __post_init__(self):
        if self.type == AggregateType.COUNT and self.field:
            raise ValueError("Field is not required for COUNT aggregate type.")
        elif (
            self.type in (AggregateType.SUM, AggregateType.DISTINCT_COUNT)
            and not self.field
        ):
            raise ValueError(
                "Field is required for SUM or DISTINCT_COUNT aggregate type."
            )


class EventAggregate:
    def __init__(
        self, name: str, event_name: str, type: AggregateType, field: str = None
    ):
        self.name = name
        self.event_name = event_name
        self.type = type
        self.field = field
        self.value = 0
        self._store = defaultdict(self._initial_value)

    def update(self, user_id: str, event: Event):
        if self.type == AggregateType.COUNT:
            self._store[user_id].add(event.uuid) # this is a set so dedupes
        elif self.type == AggregateType.SUM:
            val = self._get_event_field_value(event)
            if event.uuid not in [x[0] for x in self._store[user_id]]:
                self._store[user_id].append((event.uuid, val))
        elif self.type == AggregateType.DISTINCT_COUNT:
            self._store[user_id].add(self._get_event_field_value(event))

    def get_user_aggregate(self, user_id: str):
        if self.type == AggregateType.COUNT:
            return len(self._store.get(user_id, set()))
        elif self.type == AggregateType.DISTINCT_COUNT:
            return len(self._store.get(user_id, set()))
        elif self.type == AggregateType.SUM:
            return sum([x[1] for x in self._store.get(user_id, [])])
        else:
            raise ValueError("Invalid aggregate type.")

    def _get_event_field_value(self, event):
        val = getattr(event.event_properties, self.field, None)
        if not val:
            raise AggregationError(
                f"Field '{self.field}' not found in event properties."
            )
        return val

    def _initial_value(self):
        if self.type in (AggregateType.DISTINCT_COUNT, AggregateType.COUNT):
            return set()
        return []



class EventAggregateStore:
    def __init__(self):
        self._store: Dict[str, EventAggregate] = {}
        self._lock = asyncio.Lock()
        self._event_lookup: Dict[str, BaseModel] = {}

    def add_aggregate(self, aggregate: EventAggregate):
        if aggregate.name in self._store:
            raise AggregationError(f"Aggregate {aggregate.name} already exists.")
        self._store[aggregate.name] = aggregate
        self._index_on_event_name(aggregate)

    async def get_aggregates_by_event_name(
        self, event_name: str
    ) -> List[EventAggregate]:
        async with self._lock:
            return self._event_lookup.get(event_name, [])

    async def get_aggregate_by_name(self, name: str) -> EventAggregate:
        async with self._lock:
            if name not in self._store:
                raise ValueError(f"Aggregate {name} not found.")
            return self._store[name]

    def _index_on_event_name(self, aggregate: EventAggregate):
        if aggregate.event_name not in self._event_lookup:
            self._event_lookup[aggregate.event_name] = []
        self._event_lookup[aggregate.event_name].append(aggregate)
