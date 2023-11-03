import psycopg2
import pandas as pd
from psycopg2 import extras
from utils import postgres_secrets


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


def json_to_df(json_path: str) -> pd.DataFrame:
    stock_df = pd.read_json(json_path, lines=True)

    owners_df = pd.json_normalize(stock_df["owners"])
    resources_df = pd.json_normalize(stock_df["resources"])

    stock_df = stock_df.drop(["owners", "resources"], axis=1)

    df = pd.concat([stock_df, owners_df, resources_df], axis=1)

    df.rename(
        columns={
            "userId": "userid",
            "durationMs": "durationms",
            "POST": "post",
            "MOVIE": "movie",
            "USER_PHOTO": "user_photo",
            "GROUP_PHOTO": "group_photo",
            "group": "owners_group",
            "user": "owners_user",
            "position": 'feed_position',
            "timestamp": 'ts',
        },
        inplace=True,
    )

    return df


def df_to_db(conn, cursor, df: pd.DataFrame,):
    with conn as conn:
        with cursor as cursor:
            cursor.execute(""" TRUNCATE json_data """)

            tuples = [tuple(x) for x in df.to_numpy(na_value=None)]

            cols = ",".join(list(df.columns))

            query = "INSERT INTO %s(%s) VALUES %%s" % ("json_data", cols)
            extras.execute_values(cursor, query, tuples)

            print("the dataframe is inserted")
    conn.close()

def main():
    df = json_to_df("feeds_show.json")

    cursor, connection = create_cursor(**postgres_secrets)

    df_to_db(connection, cursor, df)


if __name__ == "__main__":
    main()
