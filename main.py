from etl.extract import extract_data
from etl.transform import clean_weather_data
from etl.load import load_to_postgres

RAW_PATH = "data/raw_weather.csv"

def main():
    print("Extracting")
    df = extract_data(RAW_PATH)

    print("Transforming...")
    df_clean = clean_weather_data(df)

    print("Loading into PostgreSQL...")
    load_to_postgres(df_clean, "weather_data")

    print("Success!")

if __name__ == "__main__":
    main()
