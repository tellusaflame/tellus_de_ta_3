import pandas as pd


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
