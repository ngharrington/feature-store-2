import asyncio

from models.event import Event

class EventProcessor:
    def __init__(self):
        pass

    async def process_event(self, event: Event):
        # Do some processing here
        await asyncio.sleep(1)
        print(f"Event {event.uuid} processed")


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
