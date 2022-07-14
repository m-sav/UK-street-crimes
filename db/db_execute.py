import psycopg2
from psycopg2.extras import RealDictCursor

USER = "postgres"
PASSWORD = "crimes"
host = "localhost"
port = "5430"
database = "ukcrimes"

def close_connection(con, cur):
    if(con):
        cur.close()
        con.close()
        # print("PostgreSQL connection is closed")

def execute_db_query(query,**kwargs):
    connection = psycopg2.connect(
            user = USER,
            password = PASSWORD,
            host = host,
            port = port,
            database=database
            )
    connection.autocommit = True

    if not connection: return

    try:
        cursor = connection.cursor(cursor_factory=RealDictCursor)
        cursor.execute(query)

        extra = cursor.rowcount if kwargs and kwargs['extras'] == 'rows_changes' else None
        if not cursor.description:
            close_connection(connection, cursor)
            to_return = extra if extra!=None else None
            return to_return

        records = cursor.fetchall()

        close_connection(connection, cursor)

        to_return = [list(map(lambda x: dict(x),records)),extra] if extra!=None else list(map(lambda x: dict(x),records))
        return to_return

    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
        close_connection(connection, cursor)
        raise Exception(error)
