import requests
from datetime import datetime
from math import floor
import json
from fastapi import FastAPI, HTTPException
from database import Database, ItemType, StatInputType
from time import sleep


URL_locations = r"https://m.avito.ru/api/1/slocations?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&limit=5&" \
                r"q={location}"


def load_json(url: str) -> dict:
    session = requests.Session()

    request = session.get(url)
    try:
        json_data = json.loads(request.text)
        return json_data
    except json.decoder.JSONDecodeError:
        raise RuntimeError(f'Доступ по ссылке {url} заблокирован')


app = FastAPI(title='avito-test')
db = Database()

@app.post('/add',
          response_description="Id добавленной связки запрос + регион")
async def add(item: ItemType):
    location_json = load_json(URL_locations.format(location=item.location))
    locations = location_json['result']['locations']
    if len(locations) == 0:
        raise HTTPException(400, 'Регион не найден')

    locationId = locations[0]['id']
    item.location = str(locationId)
    itemId = db.add(item)
    return {'status': 'ok', 'id': itemId}


@app.get('/stat',
         response_description="Счётчики и соответствующие им временные метки")
async def stat(item: StatInputType):
    timeStamp = floor(datetime.now().timestamp())

    id = db.add(item)
    return {'status': 'ok', 'id': id}
