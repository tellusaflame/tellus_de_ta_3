import os

import psycopg2
from psycopg2 import extras
import pandas as pd

from dotenv import load_dotenv

load_dotenv()

postgres_secrets = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "database": os.getenv("POSTGRES_DB"),
}


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


def json_to_df(json: str):
    stock_df = pd.read_json(json, lines=True)

    owners_df = pd.json_normalize(stock_df["owners"])
    resources_df = pd.json_normalize(stock_df["resources"])

    stock_df = stock_df.drop("owners", axis=1)
    stock_df = stock_df.drop("resources", axis=1)

    df = pd.concat([stock_df, owners_df, resources_df], axis=1)

    df.rename(
        columns={
            "userId": "userid",
            "durationMs": "durationms",
            "POST": "post",
            "MOVIE": "movie",
            "USER_PHOTO": "user_photo",
            "GROUP_PHOTO": "group_photo",
            "group": "_group",
            "user": "_user",
        },
        inplace=True,
    )

    return df


def df_to_db(conn, cursor, df):
    cursor.execute(""" TRUNCATE json_data """)
    conn.commit()

    tuples = [tuple(x) for x in df.to_numpy(na_value=None)]

    cols = ",".join(list(df.columns))

    query = "INSERT INTO %s(%s) VALUES %%s" % ("json_data", cols)
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()

    print("the dataframe is inserted")
    cursor.close()


def main():
    df = json_to_df("feeds_show.json")

    cursor, connection = create_cursor(**postgres_secrets)

    df_to_db(connection, cursor, df)


if __name__ == "__main__":
    main()
