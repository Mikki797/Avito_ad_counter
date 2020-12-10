import psycopg2
from api_types import AddInputType, StatInputType, TimestampTableType
from typing import Iterable, List, Tuple

DB_NAME = 'avito_api_db'
DB_USER = 'postgres'
DB_PASSWORD = 'admin'
DB_HOST = 'avito_test_db'
DB_PORT = '5432'


INPUT_TABLE_NAME = 'requests'
OUTPUT_TABLE_NAME = 'timeStamps'


def _create_connection(dbname, user, password, host, port):
    con = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    return con


class Database:
    @staticmethod
    def add_request(item: AddInputType) -> int:
        con = _create_connection(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        con.autocommit = True
        cursor = con.cursor()

        query = f"INSERT INTO {INPUT_TABLE_NAME} (query, locationId) values " \
            "(%(query)s, %(locationId)s) " \
            "ON CONFLICT (query, locationId) DO NOTHING " \
            "RETURNING id;"
        cursor.execute(query, {'query': item.query, 'locationId': item.location})

        result = cursor.fetchall()
        if len(result) == 0:
            query = f"SELECT id FROM {INPUT_TABLE_NAME} " \
                f"WHERE query=%(query)s and locationId=%(locationId)s;"
            cursor.execute(query, {'query': item.query, 'locationId': item.location})
            result = cursor.fetchall()
        return result[0][0]

    @staticmethod
    def get_requests(size=10) -> Iterable[List[int]]:
        con = _create_connection(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        con.autocommit = True
        cursor = con.cursor()

        query = f"SELECT * from requests"
        cursor.execute(query)

        result = cursor.fetchmany(size=size)
        yield result

        result = cursor.fetchmany(size=size)
        while result:
            yield result
            result = cursor.fetchmany(size=size)

    @staticmethod
    def add_timestamp(item: TimestampTableType) -> None:
        con = _create_connection(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        con.autocommit = True
        cursor = con.cursor()

        query = f"INSERT INTO {OUTPUT_TABLE_NAME} (requestId, timeStamp, count) values " \
            f"({item.requestId}, {item.timeStamp}, {item.count})"
        cursor.execute(query)

    @staticmethod
    def get_timestamps(item: StatInputType) -> List[Tuple[int, int]]:
        con = _create_connection(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        con.autocommit = True
        cursor = con.cursor()

        query = f"SELECT (timeStamp, count) FROM timeStamps WHERE " \
            f"requestId={item.id} and timeStamp BETWEEN {item.date1} AND {item.date2}"
        cursor.execute(query)

        result = cursor.fetchall()
        return result
