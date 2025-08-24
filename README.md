# Distributed Microservices for Market Analytics

## Overview
A scalable analytics platform for real-time and historical financial market data, built with Python, Kafka, TimescaleDB, and FastAPI. The system leverages distributed microservices to ingest, process, analyze, and serve technical indicators for stocks and other instruments. Due to API rate limiting and cost contraints, the system only supports one ticker (AAPL), but can be easily scaled to include several financial instruments. 

## Architecture Highlights
- **Distributed Microservices:** Each core function (ingestion, analytics, persistence, API, aggregation) runs as an independent, containerized service.
- **Event-Driven Processing:** Kafka streams enable asynchronous communication and analytics computation.
- **Time-Series Database:** TimescaleDB stores raw and processed OHLCV data with retention and compression policies for efficient analytics.
- **Backfill Capability:** Historical data can be replayed into the analytics pipeline to ensure complete coverage of technical indicators.
- **Extensible Indicator Framework:** Modular design for adding new technical indicators and analytics features.

## Components
- **Market Data Service:** Fetches real-time and historical OHLCV data from external APIs (Alpha Vantage, TwelveData, Yahoo Finance) and streams to Kafka.
- **Persistence Service:** Stores raw price data and analytics results in TimescaleDB.
- **Analytics Service:** Consumes price and OHLCV events, calculates technical indicators (momentum, volatility, real-time), and publishes analytics events.
- **Aggregation Service:** Aggregates lower-interval bars into higher intervals for downstream analytics.
- **API Service:** Exposes REST and WebSocket endpoints for querying prices, analytics, and available indicators.
- **Backfill Scripts:** Replay historical OHLCV data into Kafka for analytics backfill.
- **Infrastructure:** Kafka, Zookeeper, TimescaleDB, pgAdmin.

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