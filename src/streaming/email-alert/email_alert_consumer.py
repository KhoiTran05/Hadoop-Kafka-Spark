import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from kafka import KafkaConsumer
from datetime import datetime
from utils.logger_config import logger
import redis
import os
from typing import Dict, Any

EMAIL_CONFIG = {
    'smtp_server': os.getenv('SMTP_SERVER'),
    'smtp_port': int(os.getenv('SMTP_PORT', 587)),
    'sender_email': os.getenv('SENDER_EMAIL'),
    'sender_password': os.getenv('SENDER_PASSWORD'),
    'recipient_emails': os.getenv('RECIPIENT_EMAILS').split(',') if os.getenv('RECIPIENT_EMAILS') else []
}

consumer = KafkaConsumer(
    'weather-alert',
    bootstrap_servers=[os.getenv("KAFKA_BROKER_URL")],
    group_id='email-alert-group',
    auto_offset_reset='latest', 
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_DB = int(os.getenv('REDIS_DB', 0))

if not REDIS_HOST or not isinstance(REDIS_HOST, str):
    logger.error(f"Invalid REDIS_HOST: {REDIS_HOST}")
    redis_server = None
else:
    try:
        redis_server = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
            socket_timeout=5,
            socket_connect_timeout=5,
            retry_on_timeout=True
        )
  
        redis_server.ping()
        logger.info(f"Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        redis_server = None

class EmailAlertSender:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.smtp_server = None
        
    def connect_smtp(self):
        """Connect to SMTP server"""
        try:
            self.smtp_server = smtplib.SMTP(self.config['smtp_server'], self.config['smtp_port'])
            self.smtp_server.starttls()
            self.smtp_server.login(self.config['sender_email'], self.config['sender_password'])
            logger.info("Successfully connected to the SMTP server")
            return True
        except Exception as e:
            logger.error(f"SMTP connection error: {e}")
            return False
    
    def disconnect_smtp(self):
        """Disconnect from SMTP server"""
        if self.smtp_server:
            try:
                self.smtp_server.quit()
                logger.info("SMTP disconnected")
            except Exception as e:
                logger.error(f"SMTP disconnection Error: {e}")
    
    def create_email_content(self, alert_data: Dict[str, Any]) -> tuple:
        """
        Create subject and HTML body for weather alert email
        Args:
            alert_data (Dict[str, Any]): Enriched parsed weather alert message consumed from Kafka

        Returns:
            tuple: (subject, HTML content)
        """
        city = alert_data.get('city', 'Unknown')
        country = alert_data.get('country', '')
        timestamp = alert_data.get('timestamp', '') 
        alert_type = alert_data.get('alert_type', 'UNKNOWN') 

        title_map = {
            "threshold": "WEATHER WARNING: Exceeding Safety Limits",
            "change": "WEATHER WARNING: Unusual Volatility"
        }
        email_subject = f"{title_map.get(alert_type, 'ALERT')}: {city}"
        
        timestamp_html = f"<li><strong>Time Detected:</strong> {timestamp}</li>" if alert_type == "threshold" else ""
        html_header = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6;">
            <h2 style="color: #d9534f; border-bottom: 2px solid #d9534f; padding-bottom: 10px;">
                {title_map.get(alert_type, 'City Weather Alert')}
            </h2>
            
            <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 15px 0;">
                <h3>Location & Time:</h3>
                <ul>
                    <li><strong>City:</strong> {city}, {country}</li>
                    <li><strong>Alert Type:</strong> {alert_type.upper() + " ALERT"}</li>
                    {timestamp_html}
                </ul>
            </div>
            
            <div style="background-color: #fff3cd; padding: 15px; border-radius: 5px; border: 1px solid #ffeeba;">
                <h3 style="margin-top: 0;">Warning details:</h3>
                <ul style="list-style-type: none; padding: 0;">
        """

        html_body_rows = ""
        
        anomalies = alert_data.get('anomalies', [])
        
        for item in anomalies:
            metric_name = item.get('metric', 'Unknown')
            current_val = item.get('value', 'N/A')
            message = item.get('message', '')
            ts = item.get("timestamp", "")
            
            time_html = f"<br/>• Time detected: <strong>{ts}</strong>" if alert_type == "change" else ""
            row_html = f"""
            <li style="background-color: #ffffff; margin-bottom: 10px; padding: 10px; border-left: 5px solid #dc3545; box-shadow: 1px 1px 3px rgba(0,0,0,0.1);">
                <strong style="color: #dc3545; font-size: 1.1em;">{metric_name}</strong>
                <div style="margin-top: 5px;">
                    • Recognized value: <strong>{current_val}</strong><br/>
                    • Condition: <span style="color: #a71d2a;">{message}</span>
                    {time_html}
                </div>
            </li>
            """
            html_body_rows += row_html

        html_footer = """
                </ul>
            </div>
            <p style="font-size: 12px; color: #666; margin-top: 20px;">
                This is an automated message from Spark Streaming Weather System.
            </p>
        </body>
        </html>
        """

        full_html = html_header + html_body_rows + html_footer
        
        return email_subject, full_html

    
    def send_email(self, alert_data: Dict[str, Any]) -> bool:
        """
        Sending weather alert email
        Args:
            alert_data (Dict[str, Any]): Parsed weather alert message consumed from Kafka

        Returns:
            bool: True if the email was sent successfully, otherwise False.
        """
        try:
            alert_type = alert_data.get("alert_type")
            data = {
                "alert_type": alert_type,
                "city": alert_data.get("city"),
                "country": alert_data.get("country"),
                "anomalies": []
            }
            
            if alert_type == "threshold":
                data["timestamp"] = alert_data.get("event_timestamp")
                
                if alert_data["temp_anomaly"]:
                    temperature = alert_data.get("temperature")
                    data["anomalies"].append({
                        "metric": "Temperature",
                        "value": f"{temperature}°C",
                        "message": "Exceeding the safety threshold" if temperature > 0 else "Below the safety threshold"
                    })
                if alert_data["pressure_anomaly"]:
                    pressure = alert_data.get("pressure")
                    data["anomalies"].append({
                        "metric": "Pressure",
                        "value": f"{pressure}hPa",
                        "message": "Exceeding the safety threshold" if pressure >= 1033 else "Below the safety threshold"
                    })
                if alert_data["visibility_anomaly"]:
                    vis = alert_data.get("visibility")
                    data["anomalies"].append({
                        "metric": "Visibility",
                        "value": f"{vis}meter",
                        "message": "Below the safety threshold"
                    })
                if alert_data["wind_anomaly"]:
                    wind_spd = alert_data.get("wind_speed")
                    data["anomalies"].append({
                        "metric": "Wind Speed",
                        "value": f"{wind_spd}m/s",
                        "message": "Exceeding the safety threshold" 
                    })
                    
            elif alert_type == "change":
                temp_anomaly = alert_data.get("temp_change_anomaly")
                wind_anomaly = alert_data.get("wind_change_anomaly")
                press_anomaly = alert_data.get("pressure_change_anomaly")
                humid_anomaly = alert_data.get("humidity_change_anomaly")
                vis_anomaly = alert_data.get("visibility_change_anomaly")
                
                if temp_anomaly:
                    if temp_anomaly == "rise rapidly":
                        value, timestamp, message = (alert_data.get("max_temp"), alert_data.get("max_temp_time"), 
                                                        f"Sudden increase of {alert_data.get('temp_rise')}°C in the last 30 minutes")
                    else:
                        value, timestamp, message = (alert_data.get("min_temp"), alert_data.get("min_temp_time"), 
                                                        f"Sudden decrease of {alert_data.get('temp_fall')}°C in the last 30 minutes")
                    
                    data["anomalies"].append({
                        "metric": "Temperature fluctuation",
                        "value": f"{value}°C",
                        "timestamp": timestamp,
                        "message": message
                    })
                if wind_anomaly:
                    value, timestamp, message = (alert_data.get("max_wind"), alert_data.get("max_wind_time"), 
                                                    f"Sudden increase of {alert_data.get('wind_rise')}m/s in the last 30 minutes")
                    
                    data["anomalies"].append({
                        "metric": "Wind speed fluctuation",
                        "value": f"{value}m/s",
                        "timestamp": timestamp,
                        "message": message
                    })
                if press_anomaly:
                    value, timestamp, message = (alert_data.get("min_pressure"), alert_data.get("min_pressure_time"), 
                                                    f"Sudden drop of {alert_data.get('pressure_drop')}hPa in the last 30 minutes")
                    
                    data["anomalies"].append({
                        "metric": "Pressure fluctuation",
                        "value": f"{value}hPa",
                        "timestamp": timestamp,
                        "message": message
                    })
                if humid_anomaly:
                    value, timestamp, message = (alert_data.get("max_humidity"), alert_data.get("max_humid_time"), 
                                                    f"Sudden increase of {alert_data.get('humidity_rise')}% in the last 30 minutes")
                    
                    data["anomalies"].append({
                        "metric": "Humidity fluctuation",
                        "value": f"{value}%",
                        "timestamp": timestamp,
                        "message": message
                    })
                if vis_anomaly:
                    value, timestamp, message = (alert_data.get("min_visibility"), alert_data.get("min_visibility_time"), 
                                                    f"Sudden drop to below 2000 meter in the last 30 minutes")
                    
                    data["anomalies"].append({
                        "metric": "Visibility fluctuation",
                        "value": f"{value}meter",
                        "timestamp": timestamp,
                        "message": message
                    })
            
            subject, html_content = self.create_email_content(data)
            
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.config['sender_email']
            msg['To'] = ', '.join(self.config['recipient_emails'])
            
            html_part = MIMEText(html_content, 'html', 'utf-8')
            msg.attach(html_part)
            
            if not self.smtp_server:
                if not self.connect_smtp():
                    return False
            
            self.smtp_server.send_message(msg)
            logger.info(f"Alert email sent for {alert_data['city']},{alert_data['country']} "
                       f"to {len(self.config['recipient_emails'])} recipient")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False

def validate_email_config(config: Dict[str, Any]) -> bool:
    """Validate email config"""
    required_fields = ['smtp_server', 'smtp_port', 'sender_email', 'sender_password']
    for field in required_fields:
        if not config.get(field):
            logger.error(f"Email configuration missing: {field}")
            return False
    
    if not config.get('recipient_emails'):
        logger.error("Missing recipient email list")
        return False
    
    return True

def send_email_deduplicated(email_sender, alert_data, ano_list, alert_type, offset, redis_retention=1800):
    """
    Handle email deduplication before sending a weather alert.
    Args:
        email_sender (EmailAlertSender): Service responsible for sending alert emails
        alert_data (dict): Parsed weather alert message consumed from Kafka
        ano_list (tuple): Detected anomalies
        alert_type (string): Type of weather alert, either 'threshold' or 'change'
        offset (int): Kafka topic message offset
        redis_retention (int, optional): Time (in seconds) to keep deduplication keys in Redis. Defaults to 1800
    
    Returns:
        None
    """
    if not redis_server:
        logger.info("Unable to connect to Redis")
        return
    
    logger.info(f"Start email #{offset} deduplication process")
    alert_type = alert_data["alert_type"]
    city = alert_data["city"]
    
    anomalies = [anomaly for anomaly in ano_list if alert_data.get(anomaly)]
    
    for email in EMAIL_CONFIG['recipient_emails']:
        for anomaly in anomalies:
            dedup_key = f"alert_sent:{city}:{alert_type}:{anomaly}"
            
            if redis_server.exists(dedup_key):
                logger.info(f"Skipping duplicate alert for {email}:{city}:{alert_type}:{anomaly}")
                continue
            
            try:
                logger.info(f"Sending email #{offset} after deduplication process")
                success = email_sender.send_email(alert_data)

                if success:
                    logger.info(f"Alert email #{offset} sent successfully")
                else:
                    logger.info(f"Failed to sent  alert email #{offset}")
                    email_sender.disconnect_smtp()
                    if email_sender.connect_smtp():
                        logger.info("Reconnected to SMTP server")
                
                redis_server.setex(dedup_key, redis_retention, "1")
            
            except redis.exceptions.RedisError as e:
                logger.error(f"Redis error during deduplication: {e}")
            
            except Exception as e:
                logger.error(f"Error sending deduplicated email: {e}")

def main():
    logger.info("Starting email alert service ...")
    
    if not validate_email_config(EMAIL_CONFIG):
        logger.info("Invalid email configuration")
        return
    
    email_sender = EmailAlertSender(EMAIL_CONFIG)
    if not email_sender.connect_smtp():
        logger.info("Unable to connect to SMTP server")
        return
    
    logger.info(f"Email sending from: {EMAIL_CONFIG['sender_email']}")
    logger.info(f"Recipient: {', '.join(EMAIL_CONFIG['recipient_emails'])}")
    
    try:
        
        for message in consumer:
            alert_data = message.value
            offset = message.offset
            
            logger.info(f"Alert email offset {offset} received")
            
            alert_type = alert_data["alert_type"]
            
            # Email dedup
            if alert_type == "threshold":
                ano_list = ("temp_anomaly", "pressure_anomaly", 
                                "visibility_anomaly", "wind_anomaly")
                send_email_deduplicated(email_sender, alert_data, ano_list, alert_type, offset)
                
            elif alert_type == "change":
                ano_list = ("temp_change_anomaly", "wind_change_anomaly", 
                                "pressure_change_anomaly", "humidity_change_anomaly", "visibility_change_anomaly")
                send_email_deduplicated(email_sender, alert_data, ano_list, alert_type, offset)
    
    except KeyboardInterrupt:
        logger.info("\nStop Email Alert Consumer...")
    except Exception as e:
        import traceback
        logger.error(f"Error processing message: {e}")
        logger.error(traceback.format_exc())
    finally:
        email_sender.disconnect_smtp()
        consumer.close()
        logger.info("Connection closed")

if __name__ == "__main__":
    main()
