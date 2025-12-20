FROM python:3.11-slim

WORKDIR /app

COPY src/streaming/email-alert/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/streaming/email-alert/email_alert_consumer.py .
COPY utils ./utils

CMD ["python", "email_alert_consumer.py"]
