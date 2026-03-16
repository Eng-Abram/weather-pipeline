import requests
import duckdb
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

database_path = "/workspaces/weather-pipeline/weather_dbt/weather.duckdb"

api_url = (
    "https://api.open-meteo.com/v1/forecast"
    "?latitude=30.06&longitude=31.25"
    "&daily=weather_code,temperature_2m_max,temperature_2m_min,"
    "sunrise,sunset,wind_speed_10m_max"
    "&timezone=Africa%2FCairo"
    "&past_days=61"
    "&temperature_unit=fahrenheit"
)


# Feching data from the API 
def fetch_weather_data():
    print("Fetching weather data from Open-Meteo API...")
    response = requests.get(api_url, timeout=60)
    response.raise_for_status()
    data = response.json()

    return data


# Converting from json to a list of dectionaries to be be easily converted to table using duckdb
def json_to_LofDect(data):

    # from the structure of the API return, we just need the daily section:
    daily = data['daily']
    rows = []

    for i, date in enumerate(daily['time']):
        rows.append({
            'date': date,
            'weather_code' : daily['weather_code'][i],
            'temperature_2m_max' : daily['temperature_2m_max'][i],
            'temperature_2m_min' : daily['temperature_2m_min'][i],
            'sunrise' : daily['sunrise'][i],
            'sunset' : daily['sunset'][i],
            'wind_speed_10m_max' : daily['wind_speed_10m_max'][i],
            "loaded_at": datetime.now(timezone.utc).astimezone(ZoneInfo("Africa/Cairo")).isoformat()
        })

    return rows

# converting the list of dects to a table.
def load_to_duckdb(rows):
    conn = duckdb.connect(database_path)
    curs = conn.cursor()

    curs.execute(
        """CREATE TABLE IF NOT EXISTS weather_raw (
                date VARCHAR PRIMARY KEY,
                weather_code INT,
                max_temp_f DOUBLE,
                min_temp_f DOUBLE,
                sunrise VARCHAR,
                sunset VARCHAR,
                wind_speed_kmh DOUBLE,
                loaded_at VARCHAR
            )
    """
    )

    # this is for when a dag is run more than one time a day, to avoid duplicates
    curs.execute("DELETE FROM weather_raw WHERE loaded_at::DATE = CURRENT_DATE")

    # tabulating data
    for row in rows:
        curs.execute(
            "INSERT OR REPLACE INTO weather_raw(date, weather_code, max_temp_f, min_temp_f, sunrise, sunset, wind_speed_kmh, loaded_at) VALUES (?,?,?,?,?,?,?,?)",
        [row['date'], row['weather_code'], row['temperature_2m_max'], 
         row['temperature_2m_min'], row['sunrise'], row['sunset'], 
         row['wind_speed_10m_max'], row['loaded_at']]
         )

    print(f"Loaded {len(rows)} rows into raw_weather.")
    curs.close()
    conn.close()


def fetch_and_load():
    raw_data = fetch_weather_data()
    rows = json_to_LofDect(raw_data)
    load_to_duckdb(rows)

if __name__ == "__main__":
    fetch_and_load()