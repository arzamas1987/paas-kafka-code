from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional, List

from pydantic import BaseModel, Field


class Item(BaseModel):
    sku: str
    qty: int


class Address(BaseModel):
    street: str
    city: str
    zone: str


class BaseEvent(BaseModel):
    order_id: str
    event_type: str = Field(..., description="Event type name")
    order_type: Literal["delivery", "onsite", "eat-in"]
    timestamp: datetime
    correlation_id: str
    trace_id: str
    customer_id: Optional[str] = None
