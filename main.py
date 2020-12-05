import pickle
from multiprocessing.dummy import Pool as ThreadPool
from api import _load_json
import schedule
from datetime import datetime
import psycopg2
from database import INPUT_TABLE_NAME, Database as db
from api_types import TimestampTableType

URL_items = r"https://m.avito.ru/api/9/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&" \
            r"lastStamp=1606926801&locationId={locationId}&query='{query}'&page=1&limit=5"


def get_count(record):
    id, query, locationId = record

    timeStamp = int(datetime.now().timestamp())
    json_data = _load_json(URL_items.format(query=query, locationId=locationId))
    if json_data['status'] == 'ok':
        mainCount = json_data['result']['mainCount']
        item = TimestampTableType(requestId=id, timeStamp=timeStamp, count=mainCount)
        db.add_timestamp(item)


def main_func():
    for record_list in db.get_requests(size=10):
        pool = ThreadPool()
        pool.map(get_count, record_list)


schedule.every().minute.do(main_func)

while True:
    schedule.run_pending()
