import re

from fastapi import FastAPI, Header, HTTPException, status

from models.event import Event
from services.event_registry import EventTypeNotRegistered

from app_builder import event_queue, lifespan

app = FastAPI(lifespan=lifespan)


@app.get("/")
async def read_root():
    return {"Hello": "World"}


@app.post("/event")
async def publish_event(event: Event):
    try:
        event_properties_schema = await app.state.schema_registry.get_schema_by_name(
            event.name
        )
    except EventTypeNotRegistered as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )

    if not isinstance(event.event_properties, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid event format for event type {event.name}",
        )

    parsed_event = Event(
        uuid=event.uuid,
        name=event.name,
        timestamp=event.timestamp,
        event_properties=event_properties_schema(**event.event_properties),
    )

    await event_queue.put(parsed_event)
    return {"event_id": event.uuid}


@app.get("/queue-size")
async def get_queue_size():
    """
    Endpoint to return the current size of the event queue.
    """
    app.state.logger.info("getting queue size")
    try:
        queue_size = event_queue.qsize()
        return {"queue_size": queue_size}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get queue size: {str(e)}",
        )


@app.get("/{feature_flag}")
async def can_access_feature(feature_flag: str, x_user_id: str = Header(...)):
    # check if format is of the name "can<feature_name>" where featurename is lowercase ascii
    # for simplicity
    if not re.match(r"^can[a-z]{1,16}+$", feature_flag):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid feature flag"
        )
    feature_name = feature_flag[3:]
    feature = None
    try:
        feature = await app.state.feature_registry.get_feature_by_name(feature_name)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))

    has_grant = await app.state.user_feature_service.has_grant(x_user_id, feature)
    return {"user_id": x_user_id, "feature": feature.name, "has_grant": has_grant}
