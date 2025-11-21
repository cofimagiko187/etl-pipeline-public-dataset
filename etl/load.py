import pandas as pd
import psycopg2

def load_to_postgres(df: pd.DataFrame, table: str):
    conn = psycopg2.connect(
        host="localhost",
        database="weather_db",
        user="postgres",
        password="password"
    )
    cur = conn.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {table};")
    cur.execute(f"""
        CREATE TABLE {table} (
            id SERIAL PRIMARY KEY,
            station VARCHAR(255),
            date VARCHAR(50),
            max_temp FLOAT,
            min_temp FLOAT,
            precipitation FLOAT
        );
    """)

    for _, row in df.iterrows():
        cur.execute(f"""
            INSERT INTO {table} (station, date, max_temp, min_temp, precipitation)
            VALUES (%s, %s, %s, %s, %s)
        """, tuple(row))

    conn.commit()
    cur.close()
    conn.close()
