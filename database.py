import psycopg2
from psycopg2 import sql
from api_types import AddInputType, StatInputType, TimestampTableType
from typing import Iterable, List, Tuple

DATABASE_NAME = 'avito_api_test'
INPUT_TABLE_NAME = 'requests'
OUTPUT_TABLE_NAME = 'timeStamps'


def _create_connection(dbname, user, password, host, port):
    con = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    return con


def _create_database(connection, db_name=DATABASE_NAME):
    connection.autocommit = True
    cursor = connection.cursor()

    query = "CREATE DATABASE " + db_name
    cursor.execute(query)


def _create_tables(connection):
    connection.autocommit = True
    cursor = connection.cursor()

    query1 = f"""
    CREATE TABLE {INPUT_TABLE_NAME} (
        id serial PRIMARY KEY,
        query text NOT NULL,
        locationId integer NOT NULL,
        UNIQUE (query, locationId)
    );
    """

    query2 = f"""
    CREATE TABLE {OUTPUT_TABLE_NAME} (
        requestId integer REFERENCES requests,
        timeStamp integer NOT NULL,
        count integer NOT NULL,
        PRIMARY KEY (requestId, timestamp)
    );
    """
    cursor.execute(query1)
    cursor.execute(query2)


class Database:
    @staticmethod
    def add_request(item: AddInputType) -> int:
        con = _create_connection(dbname=DATABASE_NAME, user='postgres', password='admin', host='127.0.0.1', port='5432')
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
        con = _create_connection(dbname=DATABASE_NAME, user='postgres', password='admin', host='127.0.0.1', port='5432')
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
        con = _create_connection(dbname=DATABASE_NAME, user='postgres', password='admin', host='127.0.0.1', port='5432')
        con.autocommit = True
        cursor = con.cursor()

        query = f"INSERT INTO {OUTPUT_TABLE_NAME} (requestId, timeStamp, count) values " \
            f"({item.requestId}, {item.timeStamp}, {item.count})"
        cursor.execute(query)

    @staticmethod
    def get_timestamps(item: StatInputType) -> List[Tuple[int, int]]:
        con = _create_connection(dbname=DATABASE_NAME, user='postgres', password='admin', host='127.0.0.1', port='5432')
        con.autocommit = True
        cursor = con.cursor()

        query = f"SELECT (timeStamp, count) FROM timeStamps WHERE " \
            f"requestId={item.id} and timeStamp BETWEEN {item.date1} AND {item.date2}"
        cursor.execute(query)

        result = cursor.fetchall()
        return result


# con = _create_connection(dbname='postgres', user='postgres', password='admin', host='127.0.0.1', port='5432')
# _create_database(con, DATABASE_NAME)
# con = _create_connection(dbname=DATABASE_NAME, user='postgres', password='admin', host='127.0.0.1', port='5432')
# _create_tables(con)
