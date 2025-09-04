# Distributed Microservices Platform for Market Analytics

## Overview
A production-ready, event-driven analytics platform for real-time and historical financial market data processing. Built with **Python**, **Kafka**, **TimescaleDB**, and **FastAPI**, this system demonstrates enterprise-grade microservices architecture, handling high-frequency data ingestion, real-time analytics computation, and scalable API services.

**Note:** Due to API rate limiting and cost constraints, the system currently supports one ticker (AAPL) but is architected to scale to hundreds of financial instruments.

## System Architecture

### Microservices Design
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Market Data    │    │   Aggregation   │    │   Analytics     │
│    Service      │───▶│    Service      │───▶│    Service      │
│  • API Clients  │    │ • Bar Aggregator│    │ • Momentum      │
│  • Backfill Mgr │    │ • Time Windows  │    │ • Volatility    │
│  • WebSocket    │    │ • OHLCV Builder │    │ • Real-time     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                        Kafka Event Streams                      │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐    │
│  │ market.prices.  │ │ market.prices.  │ │ market.         │    │
│  │     raw         │ │     ohlcv       │ │   analytics     │    │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Persistence    │    │      API        │    │   Scripts &     │
│    Service      │    │    Service      │    │   Utilities     │
│ • Repositories  │    │ • REST Routes   │    │ • DB Init       │
│ • Migrations    │    │ • WebSockets    │    │ • Topic Create  │
│ • DB Operations │    │ • Formatters    │    │ • Health Check  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │
         ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                        TimescaleDB                              │
│            Hypertables • Compression • Retention                │
└─────────────────────────────────────────────────────────────────┘
```

### Core Components

| Service | Purpose | Key Modules | Technology Stack |
|---------|---------|-------------|------------------|
| **Market Data** | Real-time and historical data ingestion | `clients/`, `backfill/`, `repositories/` | Python, asyncio, WebSockets |
| **Aggregation** | OHLCV bar aggregation (1min → 5min → 1day) | `aggregators/bar_aggregator.py` | Python, Kafka Streams |
| **Analytics** | Technical indicators calculation | `momentum.py`, `volatility.py`, `realtime.py` | Python, NumPy, Pandas, TaLib |
| **Persistence** | Database operations and schema management | `repositories/`, `migrations/` | Python, asyncpg, TimescaleDB |
| **API** | REST/WebSocket endpoints | `routers/`, `websockets/` | FastAPI, Pydantic, uvicorn |
| **Scripts & Utils** | System initialization and utilities | `libs/`, `scripts/` | Python, PostgreSQL |

### Detailed Service Architecture

#### Market Data Service (`services/market_data/`)
```
market_data/
├── clients/
│   ├── alphavantage_client.py    # Alpha Vantage API integration
│   ├── twelvedata_client.py      # TwelveData REST API client
│   ├── twelvedata_ws_client.py   # TwelveData WebSocket client
│   └── yfinance_client.py        # Yahoo Finance fallback client
├── backfill/
│   └── backfill_manager.py       # Historical data backfilling logic
├── repositories/
│   └── ohlcv_repository.py       # OHLCV data access layer
└── service.py                    # Main service orchestrator
```

#### Analytics Service (`services/analytics/`)
```
analytics/
├── indicator/
│   ├── momentum.py               # RSI, MACD, Stochastic indicators
│   ├── volatility.py             # Bollinger Bands, ATR, Standard Dev
│   └── realtime.py               # Real-time indicator calculations
└── service.py                    # Analytics processing engine
```

#### API Service (`services/api/`)
```
api/
├── routers/
│   ├── historical.py             # Historical OHLCV endpoints
│   ├── intraday.py              # Intraday data endpoints  
│   ├── realtime.py              # Real-time price endpoints
│   └── utils/
│       ├── format_utils.py      # Response formatting utilities
│       └── timezone_utils.py    # Timezone handling utilities
├── websockets/
│   └── connection_manager.py    # WebSocket connection management
└── main.py                      # FastAPI application setup
```

#### Persistence Service (`services/persistence/`)
```
persistence/
├── repositories/
│   ├── analytics_repository.py       # Analytics data access
│   ├── ohlcv_repository.py          # OHLCV data access
│   ├── price_repository.py          # Real-time price access
│   └── realtime_analytics_repository.py # Real-time analytics access
├── migrations/
│   ├── initial_schema.py            # Database schema definitions
│   └── run_migrations.py            # Migration execution
└── service.py                       # Persistence orchestrator
```

## Data Architecture

The system is designed to handle **real-time**, **intraday**, and **historical** financial data, with a focus on efficient storage, processing, and retrieval. The architecture is optimized for time-series data using **TimescaleDB** and **Kafka** for event-driven pipelines.

### Data Flow Overview
1. **Real-Time Data**:
   - Tick data is ingested and stored in the `realtime_prices` table.
   - Real-time analytics (e.g., RSI, SMA, Bollinger Bands) are calculated and stored in the `realtime_analytics` table.

2. **Intraday Data**:
   - Tick data is aggregated into 5-minute OHLCV bars and stored in the `intraday_ohlcv` table.
   - Intraday analytics (e.g., MACD, EMA, ATR) are calculated and stored in the `intraday_analytics` table.

3. **Historical Data**:
   - Intraday OHLCV data is aggregated into daily OHLCV bars and stored in the `historical_ohlcv` table.
   - Daily analytics (e.g., long-term trends, moving averages) are calculated and stored in the `daily_analytics` table.

### Storage Tiers

1. **Real-Time Data**:
   - **Table**: `realtime_prices`
   - **Purpose**: Stores high-frequency tick data for the current trading session.
   - **Retention**: 5 days.
   - **Use Case**: Real-time analytics, WebSocket streaming.

2. **Intraday Data**:
   - **Table**: `intraday_ohlcv`
   - **Purpose**: Stores 5-minute OHLCV bars for up to 1.5 years.
   - **Retention**: 2 years.
   - **Use Case**: Intraday technical indicators, short-term analysis.

3. **Historical Data**:
   - **Table**: `historical_ohlcv`
   - **Purpose**: Stores daily OHLCV bars for long-term analysis.
   - **Retention**: 10+ years (compressed after 7 days).
   - **Use Case**: Long-term trend analysis, portfolio management.

4. **Analytics Data**:
   - **Tables**: `intraday_analytics`, `daily_analytics`, `realtime_analytics`
   - **Purpose**: Stores calculated technical indicators (e.g., RSI, MACD, Bollinger Bands).
   - **Retention**:
     - **Intraday**: 90 days.
     - **Daily**: 10+ years.
     - **Real-Time**: 1 hour.
   - **Use Case**: Indicator-based alerts, API queries.

### Database Optimization

- **Hypertables**: All time-series tables are hypertables, partitioned by time for efficient querying.
- **Compression**: Historical data is compressed after 7 days, reducing storage by ~70%.
- **Retention Policies**: Automatically removes outdated data to optimize storage.
- **Indexes**: Optimized for symbol and timestamp queries.

### Continuous Aggregates

- **5-Minute Bars**:
  - Source: `realtime_prices`
  - Target: `intraday_ohlcv`
  - Aggregation: Open, High, Low, Close, Volume (OHLCV).
  - Schedule: Every 5 minutes.

- **Daily Bars**:
  - Source: `intraday_ohlcv`
  - Target: `historical_ohlcv`
  - Aggregation: Open, High, Low, Close, Volume (OHLCV).
  - Schedule: Every 1 hour.

### Data Retention and Compression Policies

| Table                  | Retention Policy | Compression Policy |
|------------------------|------------------|--------------------|
| `realtime_prices`      | 5 days           | None               |
| `intraday_ohlcv`       | 2 years          | After 3 days       |
| `historical_ohlcv`     | 10+ years        | After 7 days       |
| `realtime_analytics`   | 1 hour           | None               |
| `intraday_analytics`   | 90 days          | After 7 days       |
| `daily_analytics`      | 10+ years        | After 30 days      |

This architecture ensures efficient storage, fast queries, and scalability for high-frequency financial data.

## Getting Started

### Prerequisites
- **Docker**: Ensure Docker is installed on your system. [Install Docker](https://docs.docker.com/get-docker/)
- **Docker Compose**: Ensure Docker Compose is installed. [Install Docker Compose](https://docs.docker.com/compose/install/)
- **Environment Variables**: Create a `.env` file in the root directory with the following variables:
  ```env
  DATABASE_URL=postgresql://postgres:password@localhost:5432/stockanalytics
  KAFKA_BOOTSTRAP_SERVERS=localhost:9092
  TWELVEDATA_API_KEY=your-twelvedata-api-key
  ALPHA_VANTAGE_API_KEY=your-alpha-vantage-api-key
  ```

### Running the System with Docker

1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd analytics-api
   ```

2. **Build and Start the Services**:
   Use Docker Compose to build and start all services:
   ```bash
   docker-compose up --build
   ```

3. **Verify Services**:
   Check that all services are running in a seperate terminal:
   ```bash
   docker-compose ps
   ```

4. **Initialize the Database**:
   Run the database initialization script to set up tables and hypertables:
   ```bash
    docker compose run --rm init-db python -m scripts.init_db
   ```

5. **Create Kafka Topics**:
   Create the required Kafka topics:
   ```bash
   docker-compose exec api python -m scripts.create_topics
   ```

6. **Access the API**:
   The API will be available at `http://localhost:8000`. You can test the health endpoint:
   ```bash
   curl http://localhost:8000/health
   ```

### Using the API

#### **Health Check**
- **Endpoint**: `GET /health`
- **Description**: Check the health of the API and connected services.
- **Example**:
  ```bash
  curl http://localhost:8000/health
  ```

#### **Real-Time Data**
- **Endpoint**: `GET /api/v2/realtime/prices/{symbol}`
- **Description**: Fetch real-time tick data for a specific stock symbol.
- **Example**:
  ```bash
  curl "http://localhost:8000/api/v2/realtime/prices/AAPL?limit=100"
  ```

#### **Intraday Data**
- **Endpoint**: `GET /api/v2/intraday/prices/{symbol}`
- **Description**: Fetch intraday OHLCV data for a specific stock symbol.
- **Example**:
  ```bash
  curl "http://localhost:8000/api/v2/intraday/prices/AAPL?interval=5min&limit=100"
  ```

#### **Historical Data**
- **Endpoint**: `GET /api/v2/historical/prices/{symbol}`
- **Description**: Fetch historical daily OHLCV data for a specific stock symbol.
- **Example**:
  ```bash
  curl "http://localhost:8000/api/v2/historical/prices/AAPL?start_date=2023-01-01&end_date=2023-12-31"
  ```

#### **WebSocket for Real-Time Updates**
- **Endpoint**: `ws://localhost:8000/ws/prices/{symbol}`
- **Description**: Subscribe to real-time price updates for a specific stock symbol.
- **Example** (using `wscat`):
  ```bash
  wscat -c ws://localhost:8000/ws/prices/AAPL
  ```

#### **WebSocket for Real-Time Analytics**
- **Endpoint**: `ws://localhost:8000/ws/analytics/{symbol}`
- **Description**: Subscribe to real-time analytics updates for a specific stock symbol.
- **Example** (using `wscat`):
  ```bash
  wscat -c ws://localhost:8000/ws/analytics/AAPL
  ```

### Stopping the System
To stop all running services:
```bash
docker-compose down
```

### Logs and Debugging
To view logs for a specific service:
```bash
docker-compose logs <service-name>
```
For example:
```bash
docker-compose logs api
```

### Additional Notes
- **Prometheus Metrics**: Metrics are available at `http://localhost:8000/metrics`.
- **Swagger Documentation**: API documentation is available at `http://localhost:8000/docs`.
- **Redoc Documentation**: Alternative API documentation is available at `http://localhost:8000/redoc`.