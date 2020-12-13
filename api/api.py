"""
JSON API Service on FastApi and uvicorn server
"""


import requests
import json

from datetime import datetime
from fastapi import FastAPI, HTTPException
from typing import Dict, Any, Optional

from api_types import AddInputType, StatInputType
from database import Database as db

URL_locations = r"https://m.avito.ru/api/1/slocations?key=f0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&limit=5&" \
                r"q={location}"


def _load_json(url: str) -> Dict[Any, Any]:
    """Private function used to get json by url"""
    session = requests.Session()

    response = session.get(url)
    try:
        json_data = response.json()
    except json.decoder.JSONDecodeError:
        raise HTTPException(403, f'Доступ по ссылке {url} заблокирован')

    if json_data.get('error') is not None:
        raise HTTPException(json_data.get('error').get('code'), json_data.get('error').get('message'))
    return json_data


app = FastAPI(title='avito-test')

@app.post('/add',
          response_description="Id добавленной связки запрос + регион")
async def add(item: AddInputType) -> Dict[str, Any]:
    """
    Add query and locationId region to database. Convert location to locationId used Avito api.

    Parameters
    ----------
    item: AddInputType from api_types
        pydantic BaseModel with fields query(str) and location(str)

    Returns
    -------
    Dict[str, Any]
        Json {'status': 'ok', 'id': itemId} where 'itemId' is id of pair query+location
    """
    location_json = _load_json(URL_locations.format(location=item.location))
    locations = location_json['result']['locations']
    if len(locations) == 0:
        raise HTTPException(400, 'Регион не найден')

    locationId = locations[0]['id']
    item.location = locationId
    itemId = db.add_request(item)
    return {'status': 'ok', 'id': itemId}


@app.get('/stat/{id}',
         response_description="Счётчики и соответствующие им временные метки")
async def stat(id: int, date1: Optional[int] = None, date2: Optional[int] = None) -> Dict[str, Any]:
    """
    Return timeStamps and count of ads on Avito for 'id' in interval from 'date1' to 'date2'.
    If 'date1' is None and 'date2' is None return all timeStamps in interval from 0 to current time.

    Parameters
    ----------
    id: int
        id of pair query+location
    date1: int, default=None
        Time stamp start of the interval. If None, then interval start from 0.
    date2: int, default=None
        Time stamp end of the interval. If None, then interval end with the current time.

    Returns
    -------
    Dict[str, Any]
        Json {'status': 'ok', 'result': result}. 'result' is array of dict like {"timeStamp": timeStamp,
        "count": count} where 'count' is number of ads on Avito in time 'timeStamp'.
    """
    if date1 is None:
        date1 = 0

    if date2 is None:
        date2 = int(datetime.now().timestamp())

    item = StatInputType(id=id, date1=date1, date2=date2)

    if item.date1 > item.date2:
        raise HTTPException(400, 'Временная метка начала интервала больше временной метки конца интервала')

    result = []
    for tup in db.get_timestamps(item):
        res: str = tup[0]
        timeStamp, count = map(int, res[1:-1].split(','))
        result.append({"timeStamp": timeStamp, "count": count})
    return {'status': 'ok', 'result': result}
