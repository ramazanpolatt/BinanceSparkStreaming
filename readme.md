# Binance Real-Time Data Pipeline

An experimental scalable real-time data engineering pipeline that streams live cryptocurrency trade data from Binance,
processes it with Spark Streaming, and monitors the infrastructure.

## 🏗️ Architecture

```text
Binance WebSocket → Python Producer → Kafka → Spark Structured Streaming -> Amazon S3 (Parquet)
                                       ↓
                            Prometheus & Grafana (Monitoring)
```

## Features

- **Real-time Ingestion**: Live BTC/USDT trade data via Binance WebSocket API.
- **Data Lake Storage**: Processed data is stored in Amazon S3 in partitioned Parquet format.
- **Message Broker**: Apache Kafka (KRaft mode) for high-throughput data decoupling.
- **Stream Processing**: Apache Spark 3.5.1 Structured Streaming for real-time data processing (Cluster usage in
  development).
- **Dependency Management**: Local Ivy cache for Spark packages to ensure fast restarts and offline capability.
- **Monitoring**: Infrastructure monitoring using Prometheus to scrape Spark metrics and Grafana for visualization.
- **Dockerized**: Fully containerized environment for consistent deployment.

## 🛠️ Tech Stack

- **Python 3.10**: Producer and Consumer logic.
- **Apache Kafka 7.6.0**: Distributed event streaming.
- **Apache Spark 3.5.1**: Distributed stream processing.
- **Prometheus**: Metrics collection.
- **Grafana**: Dashboard visualization.
- **Docker & Docker Compose**: Orchestration.

## 🚀 Getting Started

### 1. Environment Setup

```
1.  Create a file in the root directory: .env
    AWS_ACCESS_KEY_ID=your_key
    AWS_SECRET_ACCESS_KEY=your_secret
    GF_SECURITY_ADMIN_PASSWORD=your_password  (Grafana password)
```

```
2. Update the S3 bucket paths in the spark/spark_stream.py

```

```

3. Run the Pipeline
   Start all services in detached mode:
   $ docker-compose up -d

```

```
4. Verify the Flow
    
   Producer Logs: $ docker logs -f binance-producer

   Spark Processing: $ docker logs -f spark
    
   Grafana Dashboard: Open http://localhost:3000 login with admin/your_password
    
   Spark UI: Open http://localhost:4040

```

## 📂 Project Structure

- producer/: Python script to fetch data from Binance and push to Kafka.
- spark/: Spark Streaming application logic.
- ivy/: Local cache directory for Spark/Hadoop/Kafka JAR files.
- monitoring/: Configuration and provisioning for Grafana.
- prometheus.yml: Configuration for Prometheus metrics scraping.

## 🔧 Configuration Details

- Spark Ivy Cache: The project mounts ./ivy to /home/spark/.ivy2 inside the container to avoid re-downloading large JAR
  files (like Kafka SQL and AWS SDK) on every run.
- Networking: All services communicate via a dedicated internal bridge network: pipeline-net.

## 🔜 Roadmap / Future Improvements

[x] Windowed Aggregations (1-minute OHLCV-like)

[x] S3 Data Lake Integration

[ ] Move hardcoded parameters to environment variables.

[ ] Next Step: Add dbt (data build tool) for silver/gold layer transformations.

[ ] Next Step: Implement a Slack/Telegram bot for price alerts.

[ ] Dashboarding: Create comprehensive Grafana dashboards for trade analytics.

- 📄 License MIT

Status: Version 1.0 - Development in Progress
