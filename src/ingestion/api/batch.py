from utils.logger_config import logger
from utils.api_key import get_api_key
from utils.api_data_validation import validate_data
from datetime import datetime, timedelta
import requests


def get_historical_football_data(days_ago=30):
    """Ingest football historical data from Football Data API"""
    logger.info("Fetching 30 days historical football data ...")
    
    url = "https://api.football-data.org/v4/competitions/PL/matches"
    headers = {
        "X-Auth-Token": get_api_key("football")
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
            match['ingested_at'] = fetch_timestamp
        
        if historical_matches:
            validate_data('historical_football', historical_matches,  {"id", "utcDate", "homeTeam", "awayTeam", "score", "ingested_at"})
        return historical_matches
    
    except requests.RequestException as e:
        logger.error(f"AIRFLOW BATCH ERROR: Failed to fetch historical data: {e}")
        return 0
    except Exception as e:
        logger.error(f"AIRFLOW BATCH ERROR: An unexpected error occurred: {e}")
        return 0
