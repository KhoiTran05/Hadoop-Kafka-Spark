from utils.logger_config import logger
from utils.api_key import get_api_key
from utils.api_data_validation import validate_data
from datetime import datetime, timedelta
import requests
import json
import os
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
    
    start = context["data_interval_end"] - timedelta(days=14)
    end = context["data_interval_end"]

    date_from = start.date().isoformat()
    date_to = (end - timedelta(days=1)).date().isoformat()

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
    
    if not historical_matches:
            logger.warning("No matches to insert to PostgreSQL")
            return
    
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
      
def write_to_bronze_layer(**context):
    """Write raw football data to HDFS Bronze Layer"""
    logger.info("HDFS write task started")
    
    ti = context["ti"]
    file_path = ti.xcom_pull(task_ids="ingest_football_data")
    
    if not file_path:
        raise ValueError("No file path pulled from XCom")
    
    with open(file_path) as f:
        historical_matches = json.load(f)
        
    if not historical_matches:
            logger.warning("No matches to write to HDFS")
            return
        
    import pandas as pd
    from pyarrow import parquet
    from pyarrow import fs
    
    try:
        flattened_matches = []
        for match in historical_matches:
            flat_match = {
                "id": match["id"],
                "date": match["date"],
                "status": match["status"],
                'matchday': match.get('matchday'),
                'stage': match.get('stage'),
                
                'area_id': match['area']['id'],
                'area_name': match['area']['name'],
                'area_code': match['area']['code'],
                
                'competition_id': match['competition']['id'],
                'competition_name': match['competition']['name'],
                'competition_code': match['competition']['code'],

                'season_id': match['season']['id'],
                'season_start_date': match['season']['startDate'],
                'season_end_date': match['season']['endDate'],
                'current_matchday': match['season'].get('currentMatchday'),
                
                'home_team_id': match['homeTeam']['id'],
                'home_team_name': match['homeTeam']['name'],
                'home_team_short_name': match['homeTeam']['shortName'],
                'home_team_crest': match['homeTeam']['crest'],
                
                'away_team_id': match['awayTeam']['id'],
                'away_team_name': match['awayTeam']['name'],
                'away_team_short_name': match['awayTeam']['shortName'],
                'away_team_crest': match['awayTeam']['crest'],

                'winner': match['score'].get('winner'),
                'duration': match['score']['duration'],
                'fulltime_home': match['score']['fullTime'].get('home'),
                'fulltime_away': match['score']['fullTime'].get('away'),
                'halftime_home': match['score']['halfTime'].get('home'),
                'halftime_away': match['score']['halfTime'].get('away'),

                'ingested_at': match['ingested_at'],  
            }
            flattened_matches.append(flat_match)
            
        df = pd.DataFrame(flattened_matches)
        
        execution_date = context["execution_date"]
        
        df["batch_date"] = execution_date
        df["batch_year"] = execution_date.year
        df["batch_month"] = f"{execution_date.month:02d}"
        df["batch_day"] = f"{execution_date.day:02d}"
        
        data_end = context["data_interval_end"]
        data_start = data_end - timedelta(days=14)

        df["data_start_date"] = data_start.date().isoformat()
        df["data_end_date"] = (data_end - timedelta(days=1)).date().isoformat()
        
        hdfs_path = f"{os.getenv('BRONZE_LAYER_PATH')}/football"
        
        df.to_parquet(
            hdfs_path,
            partition_cols=["batch_year", "batch_month", "batch_day"],
            engine="pyarrow",
            compression="snappy",
            index=False
        )
        
        logger.info(f"Successfully wrote {len(flattened_matches)} matches to HDFS")
         
    except Exception as e:
        logger.exception(f"Failed to write Parquet to HDFS: {e}")
        raise
     
def chunked(iterable, size=500):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]
        