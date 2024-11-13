from fastapi import FastAPI
from models.event import Event
from services.event_processer import EventProcessor, EventConsumer
import asyncio
from contextlib import asynccontextmanager


NUM_CONSUMERS = 3

event_queue = asyncio.Queue()


@asynccontextmanager
async def lifespan(app: FastAPI):
    event_processor = EventProcessor()
    consumer = EventConsumer(
        queue=event_queue,
        event_processor=event_processor
    )
    consumer_tasks = [asyncio.create_task(consumer.consume()) for _ in range(NUM_CONSUMERS)]
    yield

    await event_queue.join()
    for task in consumer_tasks:
        task.cancel()
    await asyncio.gather(*consumer_tasks, return_exceptions=True)

app = FastAPI(lifespan=lifespan)


@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.post("/event")
async def publish_event(event: Event):
    await event_queue.put(event)
    return {"event_id": event.uuid}