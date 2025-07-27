# Event-Driven Microservice Stock Analytics System

## Overview
This project demonstrates a minimal event-driven microservice architecture for real-time stock analytics using Python, FastAPI, and Kafka.

## Components
- **Market Data Ingestion Service** (`src/ingestion_service.py`): Simulates stock price updates and publishes them to Kafka.
- **Analytics Service** (`src/analytics_service.py`): Consumes price events, computes a moving average, and publishes analytics events.
- **API Service** (`src/main.py`): FastAPI app to query the latest analytics.
- **Kafka + Zookeeper**: Event broker (via Docker Compose).

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Start Kafka and Zookeeper
```bash
docker-compose up
```

### 3. Start the services (in separate terminals)

#### a. Market Data Ingestion Service
```bash
python src/ingestion_service.py
```

#### b. Analytics Service
```bash
python src/analytics_service.py
```

#### c. API Service
```bash
uvicorn src.main:app --reload
```

### 4. Query Analytics
Visit [http://localhost:8000/analytics/AAPL](http://localhost:8000/analytics/AAPL) in your browser or use curl:
```bash
curl http://localhost:8000/analytics/AAPL
```

## Next Steps
- Add more symbols, analytics, persistence, alerting, authentication, etc.
- Use Docker for services.
- Add tests and monitoring.