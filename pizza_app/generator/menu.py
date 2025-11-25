from __future__ import annotations

from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class MenuItem:
    sku: str
    name: str


PIZZAS: List[MenuItem] = [
    MenuItem("pizza-margherita", "Pizza Margherita"),
    MenuItem("pizza-pepperoni", "Pizza Pepperoni"),
    MenuItem("pizza-funghi", "Pizza Funghi"),
    MenuItem("pizza-hawaii", "Pizza Hawaii"),
    MenuItem("pizza-quattro-formaggi", "Pizza Quattro Formaggi"),
    MenuItem("pizza-diavola", "Pizza Diavola"),
    MenuItem("pizza-veggie", "Pizza Vegetariana"),
    MenuItem("pizza-bbq-chicken", "Pizza BBQ Chicken"),
    MenuItem("pizza-tonno", "Pizza Tonno"),
    MenuItem("pizza-calzone", "Pizza Calzone"),
]

DRINKS: List[MenuItem] = [
    MenuItem("drink-cola", "Cola"),
    MenuItem("drink-water", "Water"),
    MenuItem("drink-sparkling-water", "Sparkling Water"),
    MenuItem("drink-lemonade", "Lemonade"),
]


def find_pizza_sku(position: int) -> str:
    return PIZZAS[position % len(PIZZAS)].sku
