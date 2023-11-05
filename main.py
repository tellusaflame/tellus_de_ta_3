from db import create_cursor, df_to_db
from jsonproc import json_to_df
from utils import postgres_secrets


def main():
    df = json_to_df("feeds_show.json")

    cursor, connection = create_cursor(**postgres_secrets)

    df_to_db(connection, cursor, df)


if __name__ == "__main__":
    main()
