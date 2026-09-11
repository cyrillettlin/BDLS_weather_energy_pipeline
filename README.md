# BDLS Weather & Energy Pipeline

A containerized big-data pipeline that ingests weather data (Open-Meteo) as well as real photovoltaic and wind farm measurements in real time, streams them via **Redpanda (Kafka-compatible)**, processes them with **Apache Spark Structured Streaming**, and stores them in **TimescaleDB**. Based on the weather data, energy yield estimates (PV & wind) are calculated and compared with the actual measurements. **Grafana** visualizes the results, while **Prometheus/cAdvisor** monitor the stack.

## Architecture

```text
                  ┌──────────────────┐
                  │  Open-Meteo API   │
                  └────────┬─────────┘
                           │
  ┌────────────────┐  ┌────▼─────────────┐  ┌────────────────────┐
  │ Solar Manager   │  │ weather-producer  │  │  DLR Wind Farm API │
  │      API        │  │                   │  │                    │
  └───────┬─────────┘  └────────┬──────────┘  └──────────┬─────────┘
          │                     │                         │
  ┌───────▼─────────┐           │              ┌──────────▼──────────┐
  │ energy-producer  │           │              │  windpark-producer  │
  └───────┬─────────┘           │              └──────────┬──────────┘
          │                     │                         │
          └───────────┬─────────┴──────────────┬──────────┘
                      │                        │
               ┌──────▼────────────────────────▼──────┐
               │        Redpanda (Kafka API)           │
               │  topics: weather-raw / energy-raw /   │
               │          windpark-raw                  │
               └──────────────────┬─────────────────────┘
                                  │
                       ┌──────────▼───────────┐
                       │ Spark: ingest_job.py │
                       │ (Structured         │
                       │  Streaming)          │
                       └──────────┬───────────┘
                                  │
                       ┌──────────▼─────────────┐
                       │       TimescaleDB       │
                       └──────────┬─────────────┘
                                  │
                       ┌──────────▼─────────────┐
                       │ Spark: estimate_job.py │
                       │ (PV-/Wind Yield        │
                       │  Estimation)            │
                       └──────────┬─────────────┘
                                  │
                  ┌───────────────┼────────────────┐
                  ▼               ▼                 ▼
              Grafana         pgAdmin         Prometheus/cAdvisor
```

## Tech Stack

| Area | Technology |
| -------------------- | ------------------------------------------------------------------------------- |
| Message Broker | [Redpanda](https://redpanda.com/) (Kafka API-compatible), including Redpanda Console |
| Stream Processing | Apache Spark 3.5 (Structured Streaming) |
| Data Storage | TimescaleDB (PostgreSQL 16 + Time-Series Extension) |
| Visualization | Grafana |
| Monitoring | Prometheus, cAdvisor, postgres-exporter |
| DB Administration | pgAdmin 4 |
| Producers | Python (Open-Meteo, Solar Manager, DLR Wind Farm) |
| Orchestration | Docker Compose |

## Prerequisites

- Docker & Docker Compose
- Credentials for the Solar Manager API (if real PV data is to be integrated)

## Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/cyrillettlin/BDLS_weather_energy_pipeline.git
   cd BDLS_weather_energy_pipeline
   ```

2. Configure environment variables:
   ```bash
   cp .env.example .env
   ```

   Then populate `.env` with your own values (see [Configuration](#configuration)).

3. Start the stack:
   ```bash
   docker compose up -d
   ```

4. Wait until all services are healthy (Redpanda and TimescaleDB have health checks; the remaining services wait for them), then open the web interfaces (see [Services & Ports](#services--ports)).

5. Stop the stack:
   ```bash
   docker compose down
   ```

   Use `-v` to additionally remove all volumes, including database contents.

## Configuration

The most important variables from `.env.example`:

| Variable | Description | Default |
| --------------------------------------------- | ---------------------------------------------------- | -------------- |
| `SOLAR_MANAGER_USER` | Login (email) for the Solar Manager API | – |
| `SOLAR_MANAGER_PASSWORD` | Password for the Solar Manager API | – |
| `SOLAR_MANAGER_SM_ID` | ID of the solar installation | – |
| `SOLAR_MANAGER_POLL_INTERVAL_SECONDS` | Polling interval of the Energy Producer | `60` |
| `PV_PEAK_POWER_W` | Rated power of the PV installation in watts | `13500` |
| `PV_PERFORMANCE_RATIO` | Expected performance ratio of the PV installation | `0.80` |
| `PV_TOLERANCE_PCT` | Tolerance band for the target/actual comparison | `0.15` |
| `PV_WEATHER_STATION_ID` | Weather station/site ID for the PV estimation | `villmergen` |
| `WIND_WEATHER_STATION_ID` | Weather station/site ID for the wind farm estimation | `krummendeich` |
| `WIND_RPM_MIN_FACTOR` / `WIND_RPM_MAX_FACTOR` | Factors for converting wind speed → rotor speed | `0.25` / `2.0` |
| `WEATHER_POLL_INTERVAL_SECONDS` | Polling interval of the Weather Producer | `900` |

## Services & Ports

| Service | Port | Description |
| -------------------------- | ------------------------------------------------------ | ------------------------------------------------------ |
| Redpanda | `19092` (Kafka), `18081` (Schema Registry), `18082` (Pandaproxy), `9644` (Admin/Metrics) | Message Broker |
| Redpanda Console | `8080` | Kafka Web UI |
| Spark Master UI | `8081` | Overview of Spark jobs |
| Spark Submit (ingest) | `4040` | Spark UI for the ingest job |
| Spark Submit (estimates) | `4041` | Spark UI for the estimate job |
| TimescaleDB | `5432` | PostgreSQL/Time-Series DB |
| pgAdmin | `5050` | DB administration (Login: `admin@admin.com` / `admin`) |
| Grafana | `3000` | Dashboards (Login: `admin` / `admin`) |
| Prometheus | `9090` | Metrics |
| cAdvisor | `8085` | Container resource metrics |
| postgres-exporter | `9187` | DB metrics for Prometheus |

> ⚠️ The default credentials for Grafana and pgAdmin are intended for local use only and should be changed before production use.

## Data Pipeline in Detail

**Producers** (`producers/`)

- `weather-producer` – periodically retrieves weather data from Open-Meteo for the configured locations and writes it to the `weather-raw` topic.
- `energy-producer` – queries the Solar Manager API for real PV yield data and writes it to the `energy-raw` topic.
- `windpark-producer` – provides wind farm/rotor speed data (DLR) to the `windpark-raw` topic.

**Spark Jobs** (`jobs/`)

- `ingest_job.py` – consumes all three Kafka topics using Structured Streaming and persists the raw data in TimescaleDB.
- `estimate_job.py` – calculates PV and wind yield estimates from the weather data and compares them with the measured actual values (including the tolerance band).

**Storage**

- `init-db/` contains the SQL scripts that are automatically executed when TimescaleDB is started for the first time (schema initialization).

**Monitoring**

- `prometheus.yml` defines the scrape targets (Redpanda, cAdvisor, postgres-exporter, Spark metrics).
- `spark-conf/metrics.properties` connects the Spark metrics to Prometheus.
- `grafana/provisioning/` contains preconfigured data sources, dashboards, and alerting rules that are automatically loaded at startup.

## Project Structure

```text
.
├── docker-compose.yml          # Orchestration of all services
├── prometheus.yml               # Prometheus configuration
├── .env.example                 # Environment variable template
├── grafana/
│   └── provisioning/            # Datasources, dashboards, alerts
├── init-db/                     # DB schema initialization
├── jobs/                        # Spark jobs (ingest_job.py, estimate_job.py, config.py)
├── producers/
│   ├── weather/                 # Open-Meteo producer
│   ├── energy/                  # Solar Manager producer
│   └── windpark/                # DLR wind farm producer
└── spark-conf/                  # Spark metrics configuration
```

## License

Not yet defined – add one if required (e.g. MIT License).
