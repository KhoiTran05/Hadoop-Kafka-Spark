from common.python.logger_config import logger
from common.python.api_rate_limiter import RateLimiter
from datetime import datetime, timedelta
from confluent_kafka import Producer
from dotenv import load_dotenv
import os
import requests
import time
import json

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

#
def get_api_key(service):
    key_map = {
        "weather": "OPENWEATHER_API_KEY",
        "football": "FOOTBALL_DATA_API_KEY"
    }
    
    if service not in key_map.keys():
        logger.error(f"API key for '{service}' not found. Check your environment variables or service name.")
        raise ValueError(f"Invalid service name: '{service}'. Must be one of {list(key_map.keys())}")
    
    env_var = key_map.get(service)
    api_key = os.getenv(env_var)
    if not api_key:
        logger.error(f"Environment variable '{env_var}' is not set. Cannot retrieve API key.")
        raise ValueError(f"Missing environment variable: {env_var}")
    
    logger.info(f"Successfully retrieved API key for '{service}'.")
    
    return api_key

# Fetch weather
def get_weather_data():
    logger.info("Fetching weather data ...")
    
    api_key = get_api_key("weather")
    
    rate_limiter = RateLimiter(APIS.get("weather").get("rate_limit"))
    weather_data = []
    
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
                "city": city,
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
            
        except Exception as e:
            logger.error(f"Error fetching weather data for {city}: {e}")
            continue
    
    logger.info(f"Successfully fetched f{len(weather_data)} weather records")
    validate_data('weather', weather_data, {'city', 'country', 'temperature', 'pressure', 'humidity', 'visibility', 'fetched_timestamp'})
    
    return weather_data

# Fetch live and upcoming football matches
def get_live_football_data():
    logger.info("Fetching live football data ...")
    
    # rate_limiter = RateLimiter(APIS.get("football").get("rate_limit"))
    # rate_limiter.wait_if_needed()
    
    url = APIS.get("football").get("url")
    headers = {
        APIS.get("football").get("headers"): get_api_key("football")
    }
    
    date_from = str(datetime.now().date())
    date_to = str((datetime.now() + timedelta(days=1)).date())
    
    live_data = []
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
            match_status = match.get("status")
            if match_status in ["LIVE", "IN_PLAY", "PAUSED", "SCHEDULED", "TIMED"]:
                live_data.append(match)
        
        logger.info(f"Successfully fetched: {len(live_data)} live and upcoming matches")
        validate_data('live_football', live_data,  {"id", "utcDate", "homeTeam", "awayTeam", "score", "fetched_timestamp"})
        
        return live_data
    
    except requests.RequestException as e:
        logger.error(f"Error fetching football data: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing football data: {e}")
        return None

# Fetch 30 days football matches n (Airflow Batch)
def get_historical_football_data(days_ago=30):
    logger.info("Fetching 30 days historical football data ...")
    
    # rate_limiter = RateLimiter(APIS.get("football").get("rate_limit"))
    # rate_limiter.wait_if_needed()
    
    url = APIS.get("football").get("url")
    headers = {
        APIS.get("football").get("headers"): get_api_key("football")
    }
    
    date_from = str((datetime.now() - timedelta(days=days_ago)).date()) 
    date_to = str(datetime.now().date())

    try:
        params = {
            "dateFrom": date_from,
            "dateTo": date_to,
            "status": '"FINISHED'
        }
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        historical_matches = data.get("matches", [])
        fetch_timestamp = datetime.now().isoformat()
        
        logger.info(f"AIRFLOW BATCH: Successfully fetched {len(historical_matches)} historical matches.")
        for match in historical_matches:
            match['fetched_timestamp'] = fetch_timestamp
            
        return historical_matches
    
    except requests.RequestException as e:
        logger.error(f"AIRFLOW BATCH ERROR: Failed to fetch historical data: {e}")
        return 0
    except Exception as e:
        logger.error(f"AIRFLOW BATCH ERROR: An unexpected error occurred: {e}")
        return 0
    
    
# Data validation
def validate_data(data_name, data, required_fields=[]):
    validation_results = {
        'timestamp': datetime.now().isoformat(),
    }
    
    if data_name == 'live_football':
        validation_results['results'] = {
            'data_name': data_name,
            'records_count': len(data),
            'has_required_fields': all(required_fields.issubset(record) for record in data),
            'status': 'PASS' if len(data) >= 0 else 'FAIL'
        }
    else:  
        validation_results['results'] = {
            'data_name': data_name,
            'records_count': len(data),
            'has_required_fields': all(required_fields.issubset(record) for record in data),
            'status': 'PASS' if len(data) > 0 else 'FAIL'
        }
    
    logger.info(f"Data validation completed for {data_name}: {validation_results}")   
    
    failed_validation = [k for k,v in validation_results['results'].items() if v['status'] == 'FAIL' or v['has_required_fields'] == False] 
    if failed_validation:
        raise ValueError(f"Data validation failed for {data_name}: {failed_validation}")
    
    return validation_results