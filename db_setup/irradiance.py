import requests
import pandas as pd
from init import connect_to_db
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta
import io

load_dotenv()

API_URL = os.getenv("API_URL")
API_KEY = os.getenv("API_KEY")

def get_irradiance(lat, long):
    """
    Pull 72 hrs data from Solcast API as a CSV
    - air temp
    - global tilted irradiance (gti)
    - precipitation_rate
    - wind_direction_10m
    - wind_speed_10m
    """
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(weeks=2)

    params = {
        "latitude": lat,
        "longitude": long,
        "start": start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "end": end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "output_parameters": "air_temp,gti,precipitation_rate,wind_direction_10m,wind_speed_10m",
        "format": "csv",
        "api_key": API_KEY
    }
    response = requests.get(API_URL, params=params)
    response.raise_for_status()

    csv_content = io.StringIO(response.content.decode('utf-8'))
    return pd.read_csv(csv_content)

def init_table():
    """
    Create a new table for solar-irradiance in PostgreSQL database.
    """
    connection = connect_to_db()
    cursor = connection.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS irradiance;")
        cursor.execute("""
            CREATE TABLE irradiance (
                timestamp TIMESTAMPTZ,
                latitude FLOAT,
                longitude FLOAT,
                air_temp FLOAT,
                gti FLOAT,
                precipitation_rate FLOAT,
                wind_direction_10m FLOAT,
                wind_speed_10m FLOAT
            );
        """)
        connection.commit()
        print("Table irradiance successfully created")
    except Exception as e:
        print(f"Error: Could not create irradiance table: {e}")
    finally:
        cursor.close()
        connection.close()

def insert_data(lat, long):
    """
    Insert data from CSV into solar-irradiance table in PostgreSQL database.
    """
    data = get_irradiance(lat, long)
    connection = connect_to_db()
    cursor = connection.cursor()
    connection.autocommit = True
    try:
        for _, row in data.iterrows():
            cursor.execute("""
                INSERT INTO irradiance (timestamp, latitude, longitude, air_temp, gti, precipitation_rate, wind_direction_10m, wind_speed_10m)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (row['period_end'], lat, long, row['air_temp'], row['gti'], row['precipitation_rate'], row['wind_direction_10m'], row['wind_speed_10m']))
        print("Data successfully inserted into irradiance table")
    except Exception as e:
        print(f"Error: Could not insert data into irradiance table: {e}")
    finally:
        cursor.close()
        connection.close()

if __name__ == "__main__":
    init_table()
    # Example coordinates, replace with actual coordinates from your route model
    coordinates = [(-33.856784, 151.215297), (36.099763, -112.112485), (51.178882, -1.826215), (41.89021, 12.492231), (29.977296, 31.132496), (27.175145, 78.042142), (48.30783, -105.1017), (34.2547, -89.8729)]
    for lat, long in coordinates:
        insert_data(lat, long)