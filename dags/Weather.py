from datetime import datetime, timedelta

import airflow
import pendulum
from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import task


Latitude = '51.5072'
Longitude = '-0.1276'
POSTGRES_CONNECTION_ID = 'postgres_default'
API_CONNECTION_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    "start_date": pendulum.now("UTC").subtract(days=1),
}

##DAG
with DAG(dag_id='Weather', default_args=default_args,schedule ='@daily', catchup= False ) as dag:
    print("hi, see you tomorrow")

    #https://open-meteo.com/en/docs?latitude=22&longitude=79
    @task
    def get_weather():
        # use connection details from Airflow connection
        http_hook = HttpHook(http_conn_id=API_CONNECTION_ID, method='GET')
        #https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current_weather=true
        ## Build API end point
        end_point = f'/v1/forecast?latitude={Latitude}&longitude={Longitude}&current_weather=true'

        #make the request
        response = http_hook.run(end_point)
        print(response)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(response.status_code)


    @task
    def transform_weather_data(weather_data):
        current_weather = weather_data['current_weather']
        transformed_weather_data = {
            'latitude': Latitude,
            'longitude':Longitude,
            'time': current_weather['time'],
            'temp': current_weather['temperature'],
            'interval': current_weather['interval'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'is_day': current_weather['is_day'],
            'weathercode': current_weather['weathercode'],
        }
        return transformed_weather_data

    @task
    def load_weather_data(transformed_weather_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONNECTION_ID)
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute("""
                    CREATE TABLE IF NOT EXISTS weather_data (
                    latitude FLOAT,
                    longitude FLOAT,
                    time TIMESTAMP,
                    temp FLOAT,
                    interval FLOAT,
                    windspeed FLOAT,
                    winddirection INT,
                    is_day INT,
                    weathercode INT);
        """)

        cur.execute("""
        INSERT INTO weather_data(
        latitude,
        longitude,
        time,
        temp,
        interval,
        windspeed,
        winddirection,
        is_day,
        weathercode) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            transformed_weather_data['latitude'],
            transformed_weather_data['longitude'],
            transformed_weather_data['time'],
            transformed_weather_data['temp'],
            transformed_weather_data['interval'],
            transformed_weather_data['windspeed'],
            transformed_weather_data['winddirection'],
            transformed_weather_data['is_day'],
            transformed_weather_data['weathercode']
        ))

        conn.commit()
        conn.close()


    def export_to_sdcard(postgresssql):



    #DAG Workflow ETL Pipeline
    weather_data_ = get_weather()
    transformed_weather_data_ = transform_weather_data(weather_data_)
    load_weather_data(transformed_weather_data_)



