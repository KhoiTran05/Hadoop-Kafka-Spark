from utils.logger_config import logger
from utils.api_key import get_api_key
from utils.api_data_validation import validate_data
from datetime import datetime, timedelta
import requests
import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from pathlib import Path
from datetime import datetime, timezone, timedelta

def get_historical_football_data(days_ago=14, **context):
    """Ingest football historical data from Football Data API"""
    logger.info("Fetching task started")
    
    url = "https://api.football-data.org/v4/competitions/PL/matches"
    headers = {
        "X-Auth-Token": get_api_key("football")
    }
    
    date_from = str((datetime.now() - timedelta(days=days_ago)).date()) 
    date_to = str((datetime.now() - timedelta(days=1)).date())

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
            match["ingested_at"] = fetch_timestamp
            
            utc_str = match["utcDate"]  
            dt_utc = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
            dt_utc7 = dt_utc.astimezone(timezone(timedelta(hours=7)))
            match["date"] = dt_utc7.isoformat()
        
        if historical_matches:
            validate_data('historical_football', historical_matches,  {"id", "utcDate", "homeTeam", "awayTeam", "score", "ingested_at"})
            
            run_id = context["run_id"]
            path = Path(f"/data/matches_{run_id}.json")
            
            with path.open("w") as f:
                json.dump(historical_matches, f)
                
            logger.info(f"/data/matches_{run_id}.json file created successfully")
            
            return str(path)
    
    except requests.RequestException as e:
        logger.error(f"AIRFLOW BATCH ERROR: Failed to fetch historical data: {e}")
        return 0
    except Exception as e:
        logger.error(f"AIRFLOW BATCH ERROR: An unexpected error occurred: {e}")
        return 0

def insert_postgres(**context):
    """Insert fetched historical matches to PostgreSQL"""
    logger.info("Insert task started")
    
    ti = context["ti"]
    file_path = ti.xcom_pull(task_ids="ingest_football_data")
    
    if not file_path:
        raise ValueError("No file path pulled from XCom")
    
    with open(file_path) as f:
        historical_matches = json.load(f)
        
    hook = PostgresHook(postgres_conn_id="postgres_football")
    conn = hook.get_conn()
    cur = conn.cursor()
    
    values = [(
        m["id"],
        m["date"],
        m["status"],
        m["homeTeam"]["id"],
        m["homeTeam"]["name"],
        m["homeTeam"]["crest"],
        m["awayTeam"]["id"],
        m["awayTeam"]["name"],
        m["awayTeam"]["crest"],
        m["score"]["duration"],
        m["score"]["fullTime"]["home"],
        m["score"]["fullTime"]["away"],
        m["score"]["halfTime"]["home"],
        m["score"]["halfTime"]["away"],
    ) for m in historical_matches]
    
    try:
        logger.info("Start inserting to postgres database ...")
        
        query = """INSERT INTO matches (
                id, event_timestamp, status, 
                home_team_id, home_team_name, home_team_crest,
                away_team_id, away_team_name, away_team_crest,
                duration, home_score, away_score,
                half_time_home, half_time_away
            ) VALUES %s
            ON CONFLICT (id) DO UPDATE
            SET 
                home_score = COALESCE(EXCLUDED.home_score, matches.home_score),
                away_score = COALESCE(EXCLUDED.away_score, matches.away_score)
            """
        
        for batch in chunked(values, size=500):
            execute_values(cur, query, batch)
            conn.commit()
        
        logger.info(f"{len(historical_matches)} matches data inserted successfully")
        
        cur.close()
        conn.close()
    except Exception as e:
        logger.exception("Failed to execute insert queries")
        raise
      
        
def chunked(iterable, size=500):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]
        