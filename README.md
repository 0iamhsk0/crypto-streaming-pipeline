# Real-Time Crypto Market Analytics Pipeline with LLM-Generated Insights 


A live streaming data pipeline that ingests real-time cryptocurrency trades from Binance, computes rolling market analytics using PySpark with sub-minute latency, detects statistical anomalies, and leverages the Gemini API to generate live natural-language market commentary.

## 🏗️ Architecture & Data Flow

```
Binance WebSocket ──▶ Python Producer ──▶ Redpanda (Kafka) ──▶ PySpark Streaming ──▶ PostgreSQL ──▶ Streamlit Dashboard ──▶ Gemini API
```

```
┌─────────────────────┐
│  Binance WebSocket  │   (live trades: BTC, ETH, BNB, SOL, XRP)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Python Producer   │   auto-reconnects on drop
└──────────┬──────────┘
           │ publishes
           ▼
┌─────────────────────┐
│  Redpanda (Kafka)   │   topic: crypto-ticks
└──────────┬──────────┘
           │ consumes
           ▼
┌───────────────────────────────────┐
│   PySpark Structured Streaming    │
│  • 1-min tumbling windows         │
│  • watermarking (late ticks)      │
│  • rolling z-score anomaly flags  │
└──────────┬────────────────────────┘
           │ upserts
           ▼
┌─────────────────────┐
│     PostgreSQL      │   market_windows table
└──────────┬──────────┘
           │ reads
           ▼
┌──────────────────────┐      ┌──────────────────┐
│  Streamlit Dashboard │◀───▶│   Gemini API     │
│  • live price/volume │ batch│  natural-language│
│  • cross-symbol view │ calls│  commentary      │
│  • anomaly markers   │      │                  │
└──────────────────────┘      └──────────────────┘
```

1. **Ingestion**: A Python producer establishes a live WebSocket connection to Binance, captures raw trade ticks for 5 major pairs (BTC, ETH, BNB, SOL, XRP), and publishes them to a Redpanda topic.
2. **Stream Processing**: PySpark Structured Streaming consumes the live stream, processes data in 1-minute tumbling windows with watermarking, and computes rolling Z-score analytics on volume and price changes to flag anomalies.
3. **Storage**: The processed window aggregates and anomaly flags are persistently upserted into a PostgreSQL database.
4. **Serving & AI Layer**: A Streamlit dashboard reads from Postgres to render real-time visualizations. It batches the latest metrics into a single structured request and calls the Gemini API, which returns one JSON response containing plain-English commentary for every symbol — generating concise market insight while staying under free-tier rate limits.

## 🛠️ Tech Stack

* **Ingestion**: Python, `websocket-client`, Binance Public API
* **Streaming Engine**: Redpanda (Kafka-API compatible), PySpark (Structured Streaming)
* **Storage**: PostgreSQL
* **Visualization**: Streamlit, Plotly
* **AI Layer**: Google Gemini API (`google-genai`)
* **Infrastructure**: Docker Compose

## 🚀 Getting Started

### Prerequisites

* Docker & Docker Compose
* Python 3.11 (Recommended to avoid dependency compilation issues)
* Java 8, 11, or 17 (Required by Apache Spark)
* A free Gemini API Key from [Google AI Studio](https://aistudio.google.com/apikey)

### 1. Installation & Environment Setup

Clone the repository and set up your Python virtual environment:

```powershell
git clone <your-repo-url>
cd crypto-streaming-pipeline

# Create and activate virtual environment
python -m venv venv
.\venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

Create a `.env` file in the root directory:

```
GEMINI_API_KEY=your_actual_gemini_api_key_here
```

### 2. Spin Up Infrastructure

Launch the Redpanda broker and PostgreSQL database containers:

```powershell
docker compose up -d
```

### 3. Run the Pipeline

Open three separate terminal windows, ensure the virtual environment is activated (`.\venv\Scripts\activate`) in each, and run the following components in order:

* **Terminal 1 (Ingestion Producer):**
```powershell
python producer/binance_producer.py
```

* **Terminal 2 (Spark Streaming Job):**
```powershell
python spark/streaming_job.py
```

* **Terminal 3 (UI Dashboard):**
```powershell
streamlit run dashboard/app.py
```

The dashboard will automatically open at `http://localhost:8501`. (Note: Allow roughly 1 minute for Spark to emit its first window aggregates to the database).

## 🎯 Key Design Trade-offs

* **Redpanda instead of Kafka**: Provides complete Kafka-API compatibility within a single lightweight container without the configuration overhead of ZooKeeper or KRaft.
* **Statistical Z-Score instead of ML**: Computes anomalous volume and price spikes dynamically against a rolling historical baseline without requiring labeled training datasets or ML model maintenance.
* **Batched AI Requests**: Combines data from all 5 ticker symbols into a single structured prompt payload, returning one JSON response with per-symbol commentary — a 5x reduction in API calls that keeps the pipeline safely under Gemini's free-tier rate limits.

## 📝 Scope & Assumptions

* **Delivery Semantics**: At-least-once delivery guaranteed through PostgreSQL idempotent upserts.
* **Schema**: Ticks are processed as raw, flexible JSON payloads rather than rigid Avro schemas.
* **Scaling**: Deployed as a single-node development cluster; built to demonstrate end-to-end data engineering architecture rather than horizontal production scaling.

## Author

Hemanth Sai Kumar — [GitHub](https://github.com/0iamhsk0)
