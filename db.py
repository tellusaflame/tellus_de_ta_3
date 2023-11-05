import psycopg2
import pandas as pd
from psycopg2 import extras


def create_cursor(**secrets):
    connection = psycopg2.connect(
        user=secrets["user"],
        password=secrets["password"],
        host=secrets["host"],
        port=secrets["port"],
        database=secrets["database"],
    )
    cursor = connection.cursor()
    return cursor, connection


def df_to_db(conn, cursor, df: pd.DataFrame, ):
    with conn as conn:
        with cursor as cursor:
            cursor.execute(""" TRUNCATE json_data """)

            tuples = [tuple(x) for x in df.to_numpy(na_value=None)]

            cols = ",".join(list(df.columns))

            query = "INSERT INTO %s(%s) VALUES %%s" % ("json_data", cols)
            extras.execute_values(cursor, query, tuples)

            print("the dataframe is inserted")
    conn.close()
