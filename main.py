import pickle
from multiprocessing.dummy import Pool as ThreadPool
from api import load_json
import schedule
from datetime import datetime

URL_items = r"https://m.avito.ru/api/9/items?key=af0deccbgcgidddjgnvljitntccdduijhdinfgjgfjir&" \
            r"lastStamp=1600000000&locationId={locationId}&query='{query}'&page=1&limit=5"


def get_count(item):
    id = item[1]

    timeStamp = int(datetime.now().timestamp())
    json_data = load_json(URL_items.format(query=item[0][0], locationId=item[0][1]))
    if json_data['status'] == 'ok':
        print(f"id: {id}, mainCount: {json_data['result']['mainCount']}, timeStamp: {timeStamp}")
    else:
        print(json_data)


def main_func():
    print(f'start - {datetime.now()}')
    with open('data.pickle', 'rb') as f:
        data = pickle.load(f)
    print(data)
    pool = ThreadPool()
    pool.map(get_count, data.items())
    print(f'finish - {datetime.now()}\n')


schedule.every().minute.do(main_func)

while True:
    schedule.run_pending()
