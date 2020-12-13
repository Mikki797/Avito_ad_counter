import schedule

from multiprocessing.dummy import Pool as ThreadPool
from datetime import datetime

from api import _load_json
from database import Database as db
from api_types import TimestampTableType

URL_items = r"https://m.avito.ru/api/9/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&" \
            r"lastStamp=1606926801&locationId={locationId}&query='{query}'&page=1&limit=5"


def get_count(record):
    """
    Get count of ads from Avito api for record (id, query, locationId) and add record like (id, timeStamp, count) to
    table 'timeStamps'

    Parameters
    ----------
    record: Tuple[int, str, int]
        Row from table 'requests' (id, query, locationId)
    """
    id, query, locationId= record

    timeStamp = int(datetime.now().timestamp())
    json_data = _load_json(URL_items.format(query=query, locationId=locationId))
    if json_data['status'] == 'ok':
        mainCount = json_data['result']['mainCount']
        item = TimestampTableType(requestId=id, timeStamp=timeStamp, count=mainCount)
        db.add_timestamp(item)


def main_func():
    """
    Get all requests from database table 'requests' and paralleling it.
    """
    for record_list in db.get_requests(size=10):
        pool = ThreadPool()
        pool.map(get_count, record_list)


schedule.every().minute.do(main_func)

while True:
    schedule.run_pending()
