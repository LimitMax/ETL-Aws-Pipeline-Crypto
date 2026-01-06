import pandas as pd

def aggregate_daily(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["symbol", "hourx"])

    agg = (
        df.groupby(["datex", "symbol", "source", "asset_type"])
          .agg(
              open_daily=("open", "first"),
              high_daily=("high", "max"),
              low_daily=("low", "min"),
              close_daily=("close", "last"),
              volume_daily=("volume", "sum"),
          )
          .reset_index()
          .rename(columns={"datex": "dt"})
    )

    return agg