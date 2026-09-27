FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY observability/ ./observability/
COPY simulators/ ./simulators/
COPY serving_api/ ./serving_api/
COPY batch_processing/ ./batch_processing/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command is overridden per-service in docker-compose.yml
CMD ["python", "-c", "print('Specify a command in docker-compose.yml')"]
