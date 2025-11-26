from utils.logger_config import logger
from utils.rate_limit import RateLimiter
from utils.api_key import get_api_key
from utils.api_data_validation import validate_data
from datetime import datetime, timedelta, timezone, time as dt_time
from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import os
import requests
import time

# Fit in API limitation
START_TIME = dt_time(0, 0, 0) 
END_TIME = dt_time(14, 0, 0) 

# Interval between API calls
WEATHER_INTERVAL_SECONDS = 5 * 60  
FOOTBALL_API_INTERVAL_SECONDS = 30

LIVE_FOOTBALL_TOPIC = "football_live"
HISTORY_FOOTBALL_TOPIC = "football_history"
WEATHER_TOPIC = "weather"
                
class DataIngestionPipeline:
    def __init__(self):
        self.schema_registry_client = SchemaRegistryClient({'url': os.getenv("SCHEMA_REGISTRY_URL")})
        self.schemas = self.get_avro_schema()
        self.football_producer = self.create_kafka_producer('football')
        self.weather_producer = self.create_kafka_producer('weather')
    
    def get_avro_schema(self):
        """Get avro schema for Schema Registry"""
        football_schema_path = '/opt/airflow/schema/football.avsc'
        weather_schema_path =  '/opt/airflow/schema/weather.avsc'
        try:
            with open(football_schema_path, 'r') as f:
                football_schema = f.read()
            with open(weather_schema_path, 'r') as f:
                weather_schema = f.read()
            return {
                'football_schema': football_schema,
                'weather_schema': weather_schema
            }
        except FileNotFoundError as e:
            logger.error(f"Avro Schema file not found: {str(e)}")
            raise
                
    def create_kafka_producer(self, producer_name):
        """
        Create Kafka SerializingProducer with AvroSerializer for value
        
        Parameters:
            producer_name (str): Kafka producer name. Must be either 'football' or 'weather'
                
        Returns
            SerializingProducer: Configured Kafka producer ready to produce message
            
        Raises:
            ValueError: If producer_name is not 'football' or 'weather'
        """
        conf = {
            'bootstrap.servers': os.getenv("KAFKA_BROKER_URL"),
            'key.serializer': StringSerializer('utf_8'),
            'compression.type': 'zstd',
            'acks': 'all'
        }
        
        if producer_name == 'football':
            football_serializer = AvroSerializer(
                schema_registry_client=self.schema_registry_client,
                schema_str=self.schemas.get('football_schema')
            )
            conf['value.serializer'] = football_serializer
            
            producer = SerializingProducer(conf=conf)
    
            return producer
        elif producer_name == 'weather':
            weather_serializer = AvroSerializer(
                schema_registry_client=self.schema_registry_client,
                schema_str=self.schemas.get('weather_schema')
            )
            conf['value.serializer'] = weather_serializer
            
            producer = SerializingProducer(conf=conf)
            
            return producer
        else:
            logger.error(f"Producer name {producer_name} not found")
            raise ValueError(f"Invalid producer name {producer_name}")
    
    def delivery_report(self, err, msg):
        """
        Callback to report the delivery status of a Kafka message
        
        Parameters:
            err (KafkaError or None): Error info if delivery failed or None if successful
            msg (Message): Kafka message object
        
        Returns:
            None
        """
        if err is not None:
            logger.error(f"Failed to deliver message: {err}")
        else:
            dt = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=7)))
            logger.info(f"{dt.strftime('%H:%M:%S')} - Topic {msg.topic()} delivered.")
        
    def produce_to_kakfa(self, producer, topic, key, value):
        """
        Produce a message to Kafka topic
        
        Parameters:
            producer (SerializingProducer): Kafka producer instance 
            topic (str): Kafka topic to which message will be sent
            key (int): Kafka message key
            value (dict): Kafka message value. Must match producer's value serializer schema 
            
        Returns:
            None
        """
        try:
            producer.poll(0)
            producer.produce(
                topic=topic,
                key=str(key),
                value=value,
                on_delivery=self.delivery_report
            )
            producer.flush()
        except Exception as e:
            logger.error(f"Error producing to Kafka: {e}")

    def get_weather_data(self):
        """Ingest weather data from Open Weather API"""
        logger.info("Fetching weather data ...")
        
        api_key = get_api_key('weather')
        
        rate_limiter = RateLimiter(60)
        cities = [
            "New York,US", "Los Angeles,US", "Chicago,US", 
            "Ha Noi,VN", "Ho Chi Minh City, VN"
        ]
        
        weather_data = []
        try:
            for city in cities:
                try:
                    rate_limiter.wait_if_needed()
                    
                    url = "https://api.openweathermap.org/data/2.5/weather"
                    request_params = {
                        "q": city,
                        "units": "metric",
                        "appid": api_key
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
                        "ingested_at": datetime.now().isoformat()
                    }
                    weather_data.append(weather_record)
                    logger.info(f"Fetched weather data for {city}")
                    
                except requests.RequestException as e:
                    logger.error(f"Error fetching weather data: {e}")
                    return None   
                
            logger.info(f"Successfully fetched {len(weather_data)} weather records")
            if weather_data:
                validate_data('weather', weather_data, 
                                {'city', 'country', 'temperature', 'pressure', 'humidity', 'visibility', 'ingested_at'})
        
        except ValueError as e:
            logger.error(f"Weather Data Validation Failed: {e}")
            return None
        except Exception as e:
            logger.error(f"Error processing weather data: {e}")
            return None
        
        return weather_data
    
    def get_football_data(self):
        """Ingest football data from Football Data API"""
        logger.info("Fetching live football data ...")
        
        url = "https://api.football-data.org/v4/competitions/PL/matches"
        headers = {
            "X-Auth-Token": get_api_key("football")
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
                match['ingested_at'] = fetch_timestamp
                status = match['status']
                if status == 'FINISHED':
                    finished_matches.append(match)
                else:
                    live_matches.append(match)
            
            if finished_matches:
                validate_data('recent_finish_football', finished_matches, 
                                   {"id", "utcDate", "homeTeam", "awayTeam", "score", "ingested_at"})
            if live_matches:
                validate_data('live_football', live_matches,  
                                   {"id", "utcDate", "homeTeam", "awayTeam", "score", "ingested_at"})
            
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
    
    def run_ingestion(self):
        """Continuously fetch football and weather data and produce messages to Kafka topics on a schedule"""
        football_last_run = datetime.min.replace(tzinfo=timezone.utc)
        weather_last_run = datetime.min.replace(tzinfo=timezone.utc)
        
        while True:
            now = datetime.now(timezone.utc)
            now_time = now.time()
            try:
                if (now - football_last_run).total_seconds() >= FOOTBALL_API_INTERVAL_SECONDS:
                    football_data = self.get_football_data()
                    live_matches = football_data.get('live')
                    finished_matches = football_data.get('recent_finish')
                    
                    if live_matches:
                        for match in live_matches:
                            self.produce_to_kakfa(self.football_producer, LIVE_FOOTBALL_TOPIC, match.get('id'), match)
                    if finished_matches:
                        for match in finished_matches:
                            self.produce_to_kakfa(self.football_producer, HISTORY_FOOTBALL_TOPIC, match.get('id'), match)
                    football_last_run = now
                
                if START_TIME <= now_time <= END_TIME and (now - weather_last_run).total_seconds() >= WEATHER_INTERVAL_SECONDS:
                    weather_data = self.get_weather_data()
                    if weather_data:
                        for record in weather_data:
                            self.produce_to_kakfa(self.weather_producer, WEATHER_TOPIC, record.get('id'), record)
                    weather_last_run = now
                    
                time.sleep(10)  
            except Exception as e:
                logger.error(f"Error during running schedule ingestion: {str(e)}")
                time.sleep(30)
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            self.football_producer.flush()
            self.weather_producer.flush()
            
            self.football_producer.close()
            self.weather_producer.close()
            logger.info("Kafka producer closed")
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

def main():
    """Execute function"""
    ingestion_pipeline = DataIngestionPipeline()
    
    try:
        ingestion_pipeline.run_ingestion()
    except Exception as e:
        logger.error(f"Error running ingestion pipeline: {str(e)}")
    finally:
        ingestion_pipeline.cleanup()
    
    
if __name__ == '__main__':
    main()