from fastapi import FastAPI, HTTPException, status, Depends
from config import get_event_properties_map
from services.event_registry import EventSchemaRegistry, EventTypeNotRegistered
from services.event_processer import EventProcessor, EventConsumer
from models.event import Event
import asyncio
from contextlib import asynccontextmanager


NUM_CONSUMERS = 25

event_queue = asyncio.Queue()


def _initialize_schema_registry():
    event_schema_registry = EventSchemaRegistry()
    event_properties_map = get_event_properties_map()
    for event_name, event_properties in event_properties_map.items():
        event_schema_registry.register_event_properties_schema(event_name, event_properties)
    return event_schema_registry

schema_registry = _initialize_schema_registry()

def get_schema_registry():
    return schema_registry


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
    try:
        event_properties_schema = await schema_registry.get_schema_by_name(event.name)
    except EventTypeNotRegistered as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    
    if not isinstance(event.event_properties, dict):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid event format for event type {event.name}")
    
    parsed_event = Event(
        uuid=event.uuid,
        name=event.name,
        timestamp=event.timestamp,
        event_properties=event_properties_schema(**event.event_properties)
    )

    await event_queue.put(parsed_event)
    return {"event_id": event.uuid}

@app.get("/queue-size")
async def get_queue_size():
    """
    Endpoint to return the current size of the event queue.
    """
    try:
        queue_size = event_queue.qsize()
        return {"queue_size": queue_size}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get queue size: {str(e)}"
        )