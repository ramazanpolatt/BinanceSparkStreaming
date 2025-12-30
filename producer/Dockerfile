FROM python:3.9-slim

WORKDIR /app

RUN pip install kafka-python-ng websocket-client

COPY binance_producer.py .

CMD ["python", "-u", "binance_producer.py"]