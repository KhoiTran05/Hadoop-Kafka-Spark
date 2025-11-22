from common.utils.logger_config import logger
from common.utils.api_rate_limiter import RateLimiter
from common.utils.api_key_getter import get_api_key
from common.utils.data_validation import validate_data
from datetime import datetime, timedelta
import requests

APIS = {
    "football": {
        "url": 'https://api.football-data.org/v4/competitions/PL/matches',
        "headers" : "X-Auth-Token",
        "rate_limit": 10
    }
}

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
            "status": 'FINISHED'
        }
        
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        
        data = response.json()
        historical_matches = data.get("matches", [])
        fetch_timestamp = datetime.now().isoformat()
        
        logger.info(f"AIRFLOW BATCH: Successfully fetched {len(historical_matches)} historical matches.")
        for match in historical_matches:
            match['fetched_timestamp'] = fetch_timestamp
        
        validate_data('live_football', historical_matches,  {"id", "utcDate", "homeTeam", "awayTeam", "score", "fetched_timestamp"})
        return historical_matches
    
    except requests.RequestException as e:
        logger.error(f"AIRFLOW BATCH ERROR: Failed to fetch historical data: {e}")
        return 0
    except Exception as e:
        logger.error(f"AIRFLOW BATCH ERROR: An unexpected error occurred: {e}")
        return 0
