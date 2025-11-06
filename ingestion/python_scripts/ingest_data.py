from common.python.logger_config import logger
from common.python.api_rate_limiter import RateLimiter
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import requests
import time
import json

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname((os.path.dirname(os.path.abspath(__file__)))))  # project root
OUTPUT_DIR = os.path.join(BASE_DIR, "data", "raw")

CITIES = [
    "New York,US", "Los Angeles,US", "Chicago,US", "Houston,US", 
    "Phoenix,US", "Philadelphia,US", "San Antonio,US", "San Diego,US",
    "Ha Noi,VN", "Ho Chi Minh City, VN"
]

STOCKS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "META", "NFLX", "NVDA"]

NEWS_CATEGORIES = ["football", "bitcoin", "technology", "game", "music"]

APIS = {
    "weather": {
        "url": "https://api.openweathermap.org/data/2.5/weather",
        "params": {
            "units": "metric"
        },
        "api_key_param": "appid",
        "rate_limit": 60 # requests per minite
    },
    "stocks": {
        "url": "https://www.alphavantage.co/query",
        "params": {
            "function": "TIME_SERIES_DAILY",
            "symbol": []
        },
        "api_key_param": "apikey",
        "rate_limit": 5
    },
    "news": {
        "url": "https://newsapi.org/v2/everything",
        "params": {
            "language": "en",
            "page_size": 100,
        },
        "api_key_param": "apiKey",
        "rate_limit": 50
    }
}

def get_api_key(service):
    key_map = {
        "weather": "OPENWEATHER_API_KEY",
        "stocks": "ALPHA_VANTAGE_API_KEY",
        "news": "NEWS_API_KEY"
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

def fetch_weather_data(**context):
    logger.info("Fetching weather data ...")
    
    api_key = get_api_key("weather")
    
    rate_limiter = RateLimiter(APIS.get("weather").get("rate_limit"))
    weather_data = []
    
    for city in CITIES:
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
                "timestamp": datetime.now().isoformat()
            }
            weather_data.append(weather_record)
            
            logger.info(f"Fetched weather data for {city}")
            
        except Exception as e:
            logger.error(f"Error fetching weather data for {city}: {e}")
            continue
    
    if weather_data:
        filepath = os.path.join(
            OUTPUT_DIR, f"weather/weather_{context['ds']}.json"
        )
        os.makedirs(os.path.dirname(filepath), exist_ok=True) #Create directory if not exists before write file
        
        with open(filepath, mode="w") as f:
            json.dump(weather_data, f, indent=2)
            
            logger.info(f"Saved {len(weather_data)} weather records to {filepath}")

    return weather_data

# "stocks": {
#         "url": "https://www.alphavantage.co/query",
#         "params": {
#             "function": "TIME_SERIES_DAILY",
#             "symbol": []
#         },
#         "api_key_param": "apikey"
#     }
def fetch_stock_data(**context):
    logger.info("Fetching stock data ...")
    
    api_key = get_api_key("stocks")
    
    rate_limiter = RateLimiter(APIS.get("stocks").get("rate_limit"))
    stock_data = []
    
    for symbol in STOCKS:
        try:
            rate_limiter.wait_if_needed()
            
            url = APIS.get("stocks").get("url")
            params = {
                "function": APIS.get("stocks").get("params").get("function"),
                "symbol": symbol,
            f"{APIS.get('stocks').get('api_key_param')}": api_key
            }
            response = requests.get(url=url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if "Time Series (Daily)" in data:
                time_series = data["Time Series (Daily)"]
                for date, value in time_series.items():
                    stock_record = {
                        "symbol": symbol,
                        "date": date,
                        "open": float(value.get("1. open")),
                        "high": float(value.get("2. high")),
                        "low": float(value.get("3. low")), 
                        "close": float(value.get("4. close")),
                        "volume": int(value.get("5. volume")),
                        "timestamp": datetime.now().isoformat()
                    }
                    stock_data.append(stock_record)

            elif "Information" in data:
                logger.info(f"{data['Information']}")
                continue
            
            logger.info(f"Fetched stock data for {symbol}")
        except Exception as e:
            logger.error(f"Error fetching stock data for {symbol}: {e}")
            continue
    
    if stock_data:
        file_path = os.path.join(
            OUTPUT_DIR, f"/stock/stock_{context['ds']}.json"
        )
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file=file_path, mode='w') as f:
            json.dump(stock_data, f, indent=2)
            logger.info(f"Saved {len(stock_data)} stock records to {file_path}")
    
    return stock_data

def fetch_news_data(**context):
    logger.info("Fetching news data ...")

    api_key = get_api_key("news")
    
    news_data = []
    rate_limiter = RateLimiter(APIS.get("news").get("rate_limit"))
    
    for category in NEWS_CATEGORIES:
        try:
            rate_limiter.wait_if_needed()
            
            url = APIS.get("news").get("url")
            params = {
                "q": category,
                "language": APIS.get("news").get("params").get("language"),
                "pageSize": APIS.get("news").get("params").get("page_size"),
                f"{APIS.get('news').get('api_key_param')}": api_key
            }
            
            response = requests.get(url=url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            if data.get("status") == "ok":
                articles = data.get("articles")
                for article in articles:
                    news_record = {
                        "source_name": article.get("source", {}).get("name"),
                        "author": article.get("author"),
                        "title": article.get("title"),
                        "description": article.get("description"),
                        "url": article.get("url"),
                        "url_to_image": article.get("urlToImage"),
                        "published_at": article.get("publishedAt"),
                        "content": article.get("content")
                    }
                    news_data.append(news_record)
                    
                logger.info(f"Fetched {len(articles)} articles for {category}") 
            elif data.get("status") == "error":
                logger.info(f"Request failed for {category}: {data.get('message')}")
                continue
        except Exception as e:
            logger.error(f"Error fetching news data for {category}: {e}")
            continue
                
    if news_data:
        file_path = os.path.join(
            OUTPUT_DIR,
            f"news/news_{context['ds']}.json"
        )     
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        with open(file_path, "w") as f:
            json.dump(news_data, f, indent=2)
            logger.info(f"Saved {len(news_data)} news record to {file_path}")
            
    return news_data

def validate_data(**context):
    validation_results = {
        'timestamp': datetime.now().isoformat(),
        'validation_date': context['ds'],
        'results': {}
    }
    
    weather_file_path = os.path.join(
        OUTPUT_DIR, f"weather/weather_{context['ds']}.json"
    )
    if os.path.exists(weather_file_path):
        with open(weather_file_path, 'r') as f:
            weather_data = json.load(f)
            
        weather_required = {'city', 'temperature', 'pressure', 'humidity'}
        validation_results['results']['weather'] = {
            'file_exists': True,
            'records_count': len(weather_data),
            'has_required_fields': all(weather_required.issubset(record) for record in weather_data[:10]),
            'status': 'PASS' if len(weather_data) > 0 else 'FAIL'
        }
    else:
        validation_results['results']['weather'] = {
            'file_exists': False,
            'status': 'FAIL'
        }
        
    stock_file_path = os.path.join(
        OUTPUT_DIR, f"stock/stock_{context['ds']}.json"
    )
    if os.path.exists(stock_file_path):
        with open(stock_file_path, 'r') as f:
            stock_data = json.load(f)
            
        stock_required = {'symbol', 'open', 'high', 'low', 'close'}
        validation_results['results']['stock'] = {
            'file_exists': True,
            'records_count': len(stock_data),
            'has_required_fields': all(stock_required.issubset(record) for record in stock_data[:10]),
            'status': 'PASS' if len(stock_data) > 0 else 'FAIL'
        }
    else:
        validation_results['results']['stock'] = {
            'file_exists': False,
            'status': 'FAIL'
        }
        
    news_file_path = os.path.join(
        OUTPUT_DIR, f"news/news_{context['ds']}.json"
    )
    if os.path.exists(news_file_path):
        with open(news_file_path, 'r') as f:
            news_data = json.load(f)
            
        news_required = {'source_name', 'title', 'content'}
        validation_results['results']['news'] = {
            'file_exists': True,
            'records_count': len(news_data),
            'has_required_fields': all(news_required.issubset(record) for record in news_data[:10]),
            'status': 'PASS' if len(news_data) > 0 else 'FAIL'
        }
    else:
        validation_results['results']['news'] = {
            'file_exists': False,
            'status': 'FAIL'
        }
        
    validation_file_path = os.path.join(
        OUTPUT_DIR, f"validation/validation_{context['ds']}"
    )
    os.makedirs(os.path.dirname(validation_file_path), exist_ok=True)
    
    with open(validation_file_path, 'w') as validation_file:
        json.dump(validation_results, validation_file, indent=2)
    
    logger.info(f"Data validation completed: {validation_results}")   
    
    failed_validation = [k for k,v in validation_results['results'].items() if v['status'] == 'FAIL'] 
    if failed_validation:
        raise ValueError(f"Data validation failed for: {failed_validation}")
    
    return validation_results