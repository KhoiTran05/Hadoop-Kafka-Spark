from common.utils.logger_config import logger
import os

# from dotenv import load_dotenv

# load_dotenv()

# Get API key
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