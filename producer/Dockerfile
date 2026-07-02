FROM python:3.11-slim

WORKDIR /app

COPY producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer/src/ ./src/
COPY schemas/ ./schemas/

CMD ["python", "src/producer.py"]