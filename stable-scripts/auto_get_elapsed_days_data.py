# Import necessary libraries
import requests
import logging
import psycopg2
import os
import time
import pandas as pd
import polars as pl
from pandas import json_normalize
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from dotenv import find_dotenv, load_dotenv

# Get the variables necessary for the database connection
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

# Set up the logging so the potential errors can be tracked and reviewed
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename=os.getenv("LOGGING_PATH"))

try:
    POSTGRES_HOST = os.getenv("POSTGRES_HOST")
    POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
    POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    API_KEY = os.getenv("API_KEY")

    # Get the date yesterday
    yesterday = datetime.strftime(datetime.now() - timedelta(days=1), "%Y%m%d")

    # Check out what is the latest date in the measurements database
    connection_url = f"postgres://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}/{POSTGRES_DATABASE}"

    # TODO: Should get the latest date per station
    checking_query = f"""
        SELECT
            station_id,
            DATE(MAX(obs_time_local)) AS latest_date
        FROM measurements

        WHERE station_id NOT IN ('IMAKAT1', 'imetroma22')

        GROUP BY station_id

        ORDER BY station_id
    """

    stations_df = pl.read_database_uri(query=checking_query, uri=connection_url)
    
    # Loop through the different stations
    for station in stations_df["station_id"].to_list():
        # Make sure that the latest_date is the same data format as yesterday
        latest_date = datetime.strftime(stations_df.filter(pl.col("station_id") == station)["latest_date"].max(), "%Y%m%d")

        if latest_date != yesterday:
            logging.info("The latest date in the database is not yesterday. Running the script...")

            # Add one day to the latest date to get the next date to be requested
            latest_date = datetime.strftime(datetime.strptime(latest_date, "%Y%m%d") + timedelta(days=1), "%Y%m%d")

            while latest_date <= yesterday:
                wunderground_url = "https://api.weather.com/v2/pws/history/all?stationId=" + station + "&format=json&units=m&date=" + latest_date + "&apiKey=" + API_KEY + "&numericPrecision=decimal"
                response = requests.get(wunderground_url)

                try:
                    data = response.json()
                    df = json_normalize(data, record_path=["observations"])
                    logging.info(f"Data from {station} have been successfully retrieved.")

                    df.rename(columns={
                        'stationID': 'station_id',
                        'obsTimeUtc': 'obs_time_utc',
                        'obsTimeLocal': 'obs_time_local',
                        'lat': 'latitude',
                        'lon': 'longitude',
                        'solarRadiationHigh': 'solar_radiation_high',
                        'uvHigh': 'uv_high',
                        'winddirAvg': 'wind_direction_avg',
                        'humidityHigh': 'humidity_high',
                        'humidityLow': 'humidity_low',
                        'humidityAvg': 'humidity_avg',
                        'qcStatus': 'qc_status',
                        'metric.tempHigh': 'temperature_high',
                        'metric.tempLow': 'temperature_low',
                        'metric.tempAvg': 'temperature_avg',
                        'metric.windspeedHigh': 'wind_speed_high',
                        'metric.windspeedLow': 'wind_speed_low',
                        'metric.windspeedAvg': 'wind_speed_avg',
                        'metric.windgustHigh': 'wind_gust_high',
                        'metric.windgustLow': 'wind_gust_low',
                        'metric.windgustAvg': 'wind_gust_avg',
                        'metric.dewptHigh': 'dew_point_high',
                        'metric.dewptLow': 'dew_point_low',
                        'metric.dewptAvg': 'dew_point_avg',
                        'metric.windchillHigh': 'wind_chill_high',
                        'metric.windchillLow': 'wind_chill_low',
                        'metric.windchillAvg': 'wind_chill_avg',
                        'metric.heatindexHigh': 'heat_index_high',
                        'metric.heatindexLow': 'heat_index_low',
                        'metric.heatindexAvg': 'heat_index_avg',
                        'metric.pressureMax': 'pressure_max',
                        'metric.pressureMin': 'pressure_min',
                        'metric.pressureTrend': 'pressure_trend',
                        'metric.precipRate': 'precipitation_rate',
                        'metric.precipTotal': 'precipitation_total',
                    }, inplace=True)

                    selected_columns = [
                        'station_id',
                        'epoch',
                        'humidity_avg',
                        'humidity_high',
                        'humidity_low',
                        'obs_time_local',
                        'obs_time_utc',
                        'solar_radiation_high',
                        'uv_high',
                        'wind_direction_avg',
                        'dew_point_avg',
                        'dew_point_high',
                        'dew_point_low',
                        'heat_index_avg',
                        'heat_index_high',
                        'heat_index_low',
                        'precipitation_rate',
                        'precipitation_total',
                        'pressure_max',
                        'pressure_min',
                        'pressure_trend',
                        'qc_status',
                        'temperature_avg',
                        'temperature_high',
                        'temperature_low',
                        'wind_chill_avg',
                        'wind_chill_high',
                        'wind_chill_low',
                        'wind_gust_avg',
                        'wind_gust_high',
                        'wind_gust_low',
                        'wind_speed_avg',
                        'wind_speed_high',
                        'wind_speed_low',
                    ]

                    # Select only the specific columns to be pushed in the measurements table
                    df_selected = df[selected_columns]

                    # Try pushing to the database
                    try:
                        conn = psycopg2.connect(
                            host=POSTGRES_HOST,
                            database=POSTGRES_DATABASE,
                            user=POSTGRES_USERNAME,
                            password=POSTGRES_PASSWORD
                        )

                        # Create the SQLAlchemy engine from the psycopg2 connection
                        cursor = conn.cursor()
                        engine = create_engine("postgresql+psycopg2://", creator=lambda: conn)

                        # Commit the new set of data for the station
                        df_selected.to_sql(
                            name="measurements",
                            con=engine,
                            if_exists="append",
                            index=False,
                            chunksize=1000
                        )

                        conn.commit()
                        logging.info(f"Data from {station} have been successfully pushed to the database.")
                    
                    except conn.IntegrityError as integrity_error:
                        logging.error(f"Error in committing new data from {station}: {integrity_error}")
                        conn.rollback()
                    
                    except Exception as exception:
                        logging.error(f"Unexpected error after the commit: {exception}")
                    
                    finally:
                        conn.close()
                        logging.info(f"Connection to the database has been closed.")

                except Exception as e:
                    logging.error(f"Error in retrieving data from {station}: {e}")
                
                # Pause for 2 seconds to prevent exceeding the API limit
                time.sleep(5)
                
                # latest_date = datetime.strftime(str(latest_date) + timedelta(days=1), "%Y%m%d")
                latest_date = (datetime.strptime(latest_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
        else:
            logging.info(f"The latest date of station {station} is yesterday. No new data to be retrieved from this station.")
            # exit()
except Exception as e:
    logging.error(f"Error in running the script after importation: {e}")