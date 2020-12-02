import typing
from pydantic import BaseModel
import pickle


class ItemType(BaseModel):
    query: str
    location: str


class StatInputType(BaseModel):
    id: int
    date1: int
    date2: int


class Database:
    def __init__(self):
        self._items: typing.Dict[(str, int), int] = {}  # item: id

    def add(self, item: ItemType) -> int:
        key = (item.query, int(item.location))
        if key in self._items:
            id = self._items[key]
        else:
            id = len(self._items) + 1
            self._items[key] = id
            with open('data.pickle', 'wb') as f:
                pickle.dump(self._items, f)
        return id
