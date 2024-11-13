from pydantic import BaseModel, field_validator
import uuid
from datetime import datetime
from typing import Dict, Any

class EventPropertiesBase(BaseModel):
    user_id: str

class ScamFlagEventProperties(EventPropertiesBase):
    pass

class AddCreditCardEventProperties(EventPropertiesBase):
    zipcode: str

class ChargebackEventProperties(EventPropertiesBase):
    amount: float

class PurchaseEventProperties(EventPropertiesBase):
    amount: float

class Event(BaseModel):
    uuid: uuid.UUID
    name: str
    timestamp: datetime
    event_properties: Any

