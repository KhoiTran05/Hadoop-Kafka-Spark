from common.utils.logger_config import logger
from common.utils.api_rate_limiter import RateLimiter
from common.utils.api_key_getter import get_api_key
from common.utils.data_validation import validate_data
from datetime import datetime, timedelta, timezone, time as dt_time
from confluent_kafka import Producer
import requests
import os
import json
import time
from dotenv import load_dotenv

load_dotenv()

WEATHER_CITIES = [
    "New York,US", "Los Angeles,US", "Chicago,US", 
    "Ha Noi,VN", "Ho Chi Minh City, VN"
]

APIS = {
    "weather": {
        "url": "https://api.openweathermap.org/data/2.5/weather",
        "params": {
            "units": "metric"
        },
        "api_key_param": "appid",
        "rate_limit": 60 # requests per minute
    },
    "football": {
        "url": 'https://api.football-data.org/v4/competitions/PL/matches',
        "headers" : "X-Auth-Token",
        "rate_limit": 10
    }
}

# 7:00 AM - 9:00 PM UTC+7 for weather API 
START_TIME = dt_time(0, 0, 0) 
END_TIME = dt_time(14, 0, 0) 

# Interval between API calls
WEATHER_INTERVAL_SECONDS = 5 * 60  
FOOTBALL_API_INTERVAL_SECONDS = 60

# API key
FOOTBALL_API_KEY = get_api_key("football")
OPENWEATHER_API_KEY = get_api_key("weather")
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL")

# Kafka topic
LIVE_FOOTBALL_TOPIC = "football_live"
HISTORY_FOOTBALL_TOPIC = "football_history"
WEATHER_TOPIC = "weather"

# Kakfa Producer
conf = {
    'bootstrap.servers': KAFKA_BROKER_URL,
    'compression.type': 'zstd',
}

producer = Producer(conf)

def delivery_report(err, msg):
    """Get message status"""
    if err is not None:
        logger.error(f"Failed to deliver message: {err}")
    else:
        logger.info(f"{datetime.now(timezone.utc).strftime('%H:%M:%S')}Z - Topic {msg.topic()} delivered.")
        
def produce_to_kakfa(topic, key, data):
    try:
        producer.poll(0)
        value = json.dumps(data).encode('utf-8')
        producer.produce(
            topic=topic,
            key=str(key).encode('utf-8'),
            value=value,
            on_delivery=delivery_report
        )
    except Exception as e:
        logger.error(f"Error producing to Kafka: {e}")
        
# Fetch weather
def get_weather_data():
    logger.info("Fetching weather data ...")
    
    api_key = OPENWEATHER_API_KEY
    
    rate_limiter = RateLimiter(APIS.get("weather").get("rate_limit"))
    weather_data = []
    
    try:
        for city in WEATHER_CITIES:
            try:
                rate_limiter.wait_if_needed()
                
                url = APIS.get("weather").get("url")
                request_params = {
                    "q": city,
                    "units": APIS.get("weather").get("params").get("units"),
                    f"{APIS.get('weather').get('api_key_param')}": api_key
                }
                
                response = requests.get(url=url, params=request_params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                weather_record = {
                    'id': data.get('id'),
                    "city": data.get('name'),
                    "country": data.get("sys", {}).get("country"),
                    "weather_main": data.get("weather", [{}])[0].get("main"),
                    "weather_description": data.get("weather", [{}])[0].get("description"),
                    "temperature": data.get("main", {}).get("temp"),
                    "pressure": data.get("main", {}).get("pressure"),
                    "humidity": data.get("main", {}).get("humidity"),
                    "visibility": data.get("visibility"),
                    "wind_speed": data.get("wind", {}).get("speed"),
                    "fetched_timestamp": datetime.now().isoformat()
                }
                weather_data.append(weather_record)
                logger.info(f"Fetched weather data for {city}")
                
            except requests.RequestException as e:
                logger.error(f"Error fetching weather data: {e}")
                return None   
        logger.info(f"Successfully fetched {len(weather_data)} weather records")
        validate_data('weather', weather_data, {'city', 'country', 'temperature', 'pressure', 'humidity', 'visibility', 'fetched_timestamp'})
    
    except ValueError as e:
        logger.error(f"Football Data Validation Failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing football data: {e}")
        return None
    
    return weather_data

# Fetch football
def get_football_data():
    logger.info("Fetching live football data ...")
    # rate_limiter = RateLimiter(APIS.get("football").get("rate_limit"))
    # rate_limiter.wait_if_needed()
    url = APIS.get("football").get("url")
    headers = {
        APIS.get("football").get("headers"): FOOTBALL_API_KEY
    }
    
    date_from = str(datetime.now().date())
    date_to = str((datetime.now() + timedelta(days=1)).date())
    
    live_matches = []
    finished_matches = []
    try:
        params = {
            "dateFrom": date_from,
            "dateTo": date_to
        }
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        all_matches = data.get("matches", [])
        
        fetch_timestamp = datetime.now().isoformat()
        for match in all_matches:
            match['fetched_timestamp'] = fetch_timestamp
            status = match['status']
            if status == 'FINISHED':
                finished_matches.append(match)
            else:
                live_matches.append(match)
        
        if finished_matches:
            validate_data('recent_finish_football', finished_matches, {"id", "utcDate", "homeTeam", "awayTeam", "score", "fetched_timestamp"})
        validate_data('live_football', live_matches,  {"id", "utcDate", "homeTeam", "awayTeam", "score", "fetched_timestamp"})
        
        logger.info(f"Fetched {len(all_matches)} total, {len(live_matches)} LIVE/SCHEDULED, {len(finished_matches)} FINISHED.")
        return {'live': live_matches, 'recent_finish': finished_matches}
    except requests.RequestException as e:
        logger.error(f"Error fetching football data: {e}")
        return None
    except ValueError as e:
        logger.error(f"Football Data Validation Failed: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing football data: {e}")
        return None
    
# Execute API calls and produce to Kafka
def main_loop():
    football_last_run = datetime.min.replace(tzinfo=timezone.utc)
    weather_last_run = datetime.min.replace(tzinfo=timezone.utc)
    
    logger.info(f"Producer Service Started. Connecting to Kafka at {KAFKA_BROKER_URL}")
    while True:
        now = datetime.now(timezone.utc)
        now_time = now.time()
        
        # Produce football data 
        if (now - football_last_run).total_seconds() >= FOOTBALL_API_INTERVAL_SECONDS:
            football_data = get_football_data()
            live_matches = football_data.get('live')
            finished_matches = football_data.get('recent_finish')
            
            if live_matches:
                for match in live_matches:
                    produce_to_kakfa(LIVE_FOOTBALL_TOPIC, match.get('id'), match)
            if finished_matches:
                for match in finished_matches:
                    produce_to_kakfa(HISTORY_FOOTBALL_TOPIC, match.get('id'), match)
            football_last_run = now
        
        # Produce weather data 
        if START_TIME <= now_time <= END_TIME and (now - weather_last_run).total_seconds() >= WEATHER_INTERVAL_SECONDS:
            weather_data = get_weather_data()
            if weather_data:
                for record in weather_data:
                    produce_to_kakfa(WEATHER_TOPIC, record.get('id'), record)
            weather_last_run = now
            
        time.sleep(5)  
            
            
if __name__ == '__main__':
    main_loop()
                