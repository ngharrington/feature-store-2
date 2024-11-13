import asyncio

from models.event import Event
from models.aggregate import EventAggregateStore

class EventProcessor:
    def __init__(self, aggregate_store: EventAggregateStore):
        self.agg_store = aggregate_store

    async def process_event(self, event: Event):
        aggregates = await self.agg_store.get_aggregates_by_event_name(event.name)
        for agg in aggregates:
            agg.update(event.event_properties.user_id, event)


        print("processed event")


class EventConsumer:
    def __init__(self, queue: asyncio.Queue, event_processor: EventProcessor):
        self.queue = queue
        self.event_processor = event_processor

    async def consume(self):
        try:
            while True:
                print("waiting")
                item = await self.queue.get()
                print("got item")
                await self.event_processor.process_event(item)
                self.queue.task_done()
        except asyncio.CancelledError:
            print("Consumer cancelled")
            return
        except Exception as e:
            print(f"Consumer error: {e}")
            raise
