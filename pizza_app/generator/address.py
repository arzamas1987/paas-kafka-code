from __future__ import annotations

import random
from dataclasses import dataclass
from typing import List

from pizza_app.models.base import Address


@dataclass(frozen=True)
class Street:
    name: str


STREETS: List[Street] = [
    Street("Oven Lane"),
    Street("Mozzarella Street"),
    Street("Pepperoni Road"),
    Street("Basil Avenue"),
    Street("Dough Boulevard"),
    Street("Tomato Alley"),
]

ZONES = ["north", "south", "east", "west", "center"]


def generate_address(rng: random.Random) -> Address:
    street = rng.choice(STREETS)
    house_number = rng.randint(1, 99)
    zone = rng.choice(ZONES)
    return Address(
        street=f"{street.name} {house_number}",
        city="Pizzaburg",
        zone=zone,
    )
