import pandas as pd

def clean_weather_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna()
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]
    return df
