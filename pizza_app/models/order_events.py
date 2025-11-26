from __future__ import annotations

from datetime import datetime
from typing import Optional, List, Literal

from pydantic import Field

from .base import BaseEvent, Item, Address


class OrderReceived(BaseEvent):
    event_type: Literal["OrderReceived"] = "OrderReceived"
    items: List[Item]
    address: Optional[Address] = None
    table_number: Optional[int] = None


class OrderValidated(BaseEvent):
    event_type: Literal["OrderValidated"] = "OrderValidated"
    items: List[Item]
    address: Optional[Address] = None
    is_address_serviceable: Optional[bool] = None
    kitchen_window: Optional[str] = None
    validation_notes: List[str] = Field(default_factory=list)


class OrderPrepared(BaseEvent):
    event_type: Literal["OrderPrepared"] = "OrderPrepared"
    prep_started_at: datetime
    estimated_oven_slot: Optional[str] = None
    items: List[Item]


class OrderBaked(BaseEvent):
    event_type: Literal["OrderBaked"] = "OrderBaked"
    baked_items: List[str]
    baked_at: datetime


class OrderReadyForPickup(BaseEvent):
    event_type: Literal["OrderReadyForPickup"] = "OrderReadyForPickup"
    pickup_point: str
    ready_at: datetime


class OrderPickedUp(BaseEvent):
    event_type: Literal["OrderPickedUp"] = "OrderPickedUp"
    courier_id: str
    picked_up_at: datetime


class OrderDelivered(BaseEvent):
    event_type: Literal["OrderDelivered"] = "OrderDelivered"
    delivered_at: datetime
    customer_feedback: Optional[str] = None


class OrderCompleted(BaseEvent):
    event_type: Literal["OrderCompleted"] = "OrderCompleted"
    completed_at: datetime
