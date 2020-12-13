import psycopg2

from typing import Iterable, List, Tuple

from api_types import AddInputType, StatInputType, TimestampTableType

DB_NAME = 'avito_api_test'
DB_USER = 'postgres'
DB_PASSWORD = 'admin'
DB_HOST = '127.0.0.1'
DB_PORT = '5432'

INPUT_TABLE_NAME = 'requests'
OUTPUT_TABLE_NAME = 'timeStamps'


def _create_connection(dbname, user, password, host, port):
    """
    Get connect to database

    Parameters
    ----------
    dbname: str
        Database name to connect
    user: str
        User name database
    password: str
        Database password
    host: str
        Database: host (name of docker container if docker used)
    port: str
        Port to connect to database
    """
    con = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    return con


class Database:
    """
    Provides access to database on PostgreSQL
    """
    @staticmethod
    def add_request(item: AddInputType) -> int:
        """
        Try to add item with fields query (str) and locationId (str) to table 'requests'. If item is already in table
        return its 'id', else add item and return 'id'.

        Parameters
        ----------
        item: AddInputType from api_types
            pydantic BaseModel with fields query(str) and locationId(str)

        Returns
        -------
        int
            id of item in database
        """
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
        """
        Connect to database and get rows from table 'requests'.

        Parameters
        ----------
        size: int, default=10
            Size of list in generator.

        Returns
        -------
        Iterable[List[int]]
            Generator of list size of 'size' which contains rows (id, query, locationId) from table 'requests'
        """
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
        """
        Added 'item' to table 'timestamps'

        Parameters
        ----------
        item: TimestampTableType from api_types
            pydantic BaseModel with fields requestId (int), timeStamp (int), count (int)
        """
        con = _create_connection(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        con.autocommit = True
        cursor = con.cursor()

        query = f"INSERT INTO {OUTPUT_TABLE_NAME} (requestId, timeStamp, count) values " \
            f"({item.requestId}, {item.timeStamp}, {item.count})"
        cursor.execute(query)

    @staticmethod
    def get_timestamps(item: StatInputType) -> List[Tuple[str, ]]:
        """
        Get stat for 'item' from table 'timestamps'. item.id is id of request, item.date1 is timestamp start of
        interval, item.date2 is timestamp end of the interval.

        Parameters
        ----------
        item: TimestampTableType from api_types
            pydantic BaseModel with fields id (int), date1 (int), date2 (int).

        Returns
        -------
        List[Tuple[str, ]]
            Tuples contains str like '(timeStamp,count)'
        """
        con = _create_connection(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT)
        con.autocommit = True
        cursor = con.cursor()

        query = f"SELECT (timeStamp, count) FROM timeStamps WHERE " \
            f"requestId={item.id} and timeStamp BETWEEN {item.date1} AND {item.date2}"
        cursor.execute(query)

        result = cursor.fetchall()
        return result
