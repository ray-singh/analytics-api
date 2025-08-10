# Event-Driven Microservice Stock Analytics System

## Overview
This project implements an event-driven microservice architecture for real-time and historical stock analytics using Python, FastAPI, Kafka, and TimescaleDB.

## Components
- **Market Data Ingestion Service** (`src/ingestion_service.py`): Fetches historical data, streams real-time stock prices, and publishes them to Kafka.
- **Analytics Service** (`src/analytics_service.py`): Consumes price events, calculates a comprehensive suite of technical indicators (e.g., SMA, EMA, RSI, MACD, Bollinger Bands), and stores analytics in the database.
- **API Service** (`src/main.py`): FastAPI app exposing endpoints to query real-time and historical analytics.
- **Database** (`src/db.py`): TimescaleDB with a three-tier architecture for real-time, intraday, and historical data storage, optimized for time-series analytics.
- **Kafka + Zookeeper**: Event broker for real-time data streaming and analytics.

## Features
- **Real-Time Data Streaming**: Streams live stock prices via WebSocket and publishes them to Kafka.
- **Historical Data Backfill**: Smart backfill logic to fetch only missing historical data.
- **Technical Indicators**: Calculates indicators like SMA, EMA, RSI, MACD, and Bollinger Bands for intraday and daily timeframes.
- **Three-Tier Data Architecture**:
  - `realtime_prices`: High-frequency, volatile data (current session).
  - `intraday_ohlcv`: 5-minute OHLCV data (past 1 and a half years).
  - `historical_ohlcv`: Daily OHLCV data (10+ years).
- **Analytics Storage**:
  - `intraday_analytics`: Stores intraday technical indicators (5-minute intervals).
  - `daily_analytics`: Stores daily technical indicators.
- **API Endpoints**: Query analytics and data via RESTful endpoints.

## Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Start Kafka and Zookeeper
```bash
docker-compose up
```

### 3. Start the Services (in Separate Terminals)

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

### 4. Initialize the Database
Run the following to set up the database schema and TimescaleDB hypertables:
```bash
python -c "from src.db import init_db; init_db()"
```

### 5. Query Analytics
Visit [http://localhost:8000/analytics/AAPL](http://localhost:8000/analytics/AAPL) in your browser or use curl:
```bash
curl http://localhost:8000/analytics/AAPL
```

## API Endpoints

### Data Endpoints
- `GET /api/data/{symbol}?interval=5min&start=1627776000&end=1659312000`: Query raw OHLCV data.
- `GET /api/ohlc/{symbol}?interval=1day&limit=100`: Query historical OHLCV data.

### Analytics Endpoints
- `GET /api/analytics/intraday/{symbol}`: Query intraday technical indicators.
- `GET /api/analytics/daily/{symbol}`: Query daily technical indicators.
- `GET /api/indicators/{symbol}?type=rsi&period=14`: Query specific indicators.

## Architecture

### Data Flow
1. **Ingestion**: Fetches historical and real-time data, publishing price events to Kafka.
2. **Analytics**: Consumes price events, calculates technical indicators, and stores results in the database.
3. **API**: Exposes endpoints for querying real-time and historical analytics.

### Database Design
- **TimescaleDB**: Optimized for time-series data with hypertables and chunking.
- **Three-Tier Architecture**:
  - `realtime_prices`: Stores high-frequency real-time data.
  - `intraday_ohlcv`: Consolidates real-time data into 5-minute OHLCV intervals.
  - `historical_ohlcv`: Stores daily OHLCV data for long-term analysis.
- **Analytics Tables**:
  - `intraday_analytics`: Stores intraday technical indicators.
  - `daily_analytics`: Stores daily technical indicators.

## Example Usage

### Query Intraday Analytics
```bash
curl http://localhost:8000/api/analytics/intraday/AAPL
```

### Query Daily Analytics
```bash
curl http://localhost:8000/api/analytics/daily/AAPL
```

### Query Specific Indicator (e.g., RSI)
```bash
curl "http://localhost:8000/api/indicators/AAPL?type=rsi&period=14"
```

## Next Steps
- Implement alerting for specific indicator thresholds.
- Add authentication and authorization for API endpoints.
- Use Docker to containerize all services for easier deployment.
- Add monitoring and logging for production readiness.