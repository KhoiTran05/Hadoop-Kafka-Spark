import logging

#Create logger 
logger = logging.getLogger("app_logger")
logger.setLevel(logging.INFO)

#Create log file handler
file_handler = logging.FileHandler("app.log")
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)