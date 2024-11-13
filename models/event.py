from pydantic import BaseModel
import uuid
from datetime import datetime
from typing import Dict

class Event(BaseModel):
    uuid: uuid.UUID
    name: str
    timestamp: datetime
    event_properties: Dict
