import typing
from pydantic import BaseModel


class AddInputType(BaseModel):
    query: str
    location: str


class StatInputType(BaseModel):
    id: int
    date1: int
    date2: int


class TimestampTableType(BaseModel):
    requestId: int
    timeStamp: int
    count: int