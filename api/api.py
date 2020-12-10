import requests
import json
from fastapi import FastAPI, HTTPException
from api_types import AddInputType, StatInputType
from database import Database as db
from typing import Dict, Any
from typing import Optional
from datetime import datetime


URL_locations = r"https://m.avito.ru/api/1/slocations?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&limit=5&" \
                r"q={location}"


def _load_json(url: str) -> Dict[Any, Any]:
    session = requests.Session()

    response = session.get(url)
    try:
        json_data = response.json()
        return json_data
    except json.decoder.JSONDecodeError:
        raise RuntimeError(f'Доступ по ссылке {url} заблокирован')


app = FastAPI(title='avito-test')

@app.post('/add',
          response_description="Id добавленной связки запрос + регион")
async def add(item: AddInputType) -> Dict[str, Any]:
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
