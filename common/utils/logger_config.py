import logging

#Create logger 
logger = logging.getLogger("app_logger")
logger.setLevel(logging.INFO)

console = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)

logger.handlers.clear()     
logger.addHandler(console)  
