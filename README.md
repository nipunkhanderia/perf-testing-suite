# perf-testing-suite

![CI](https://github.com/nipunkhanderia/perf-testing-suite/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue)
![JMeter](https://img.shields.io/badge/JMeter-5.6-red)
![Locust](https://img.shields.io/badge/Locust-2.24-green)
![Kafka](https://img.shields.io/badge/Kafka-7.5-black)

**Performance QA suite for a flight operations data pipeline — tests Kafka ingestion, Lambda processing, and schema validation under load using JMeter and Locust, covering both component isolation and full E2E throughput scenarios.**

---

## 🏗️ Pipeline Architecture

```
Flight Ops System
      │
      ▼
[Kafka Topic: flight-operations]   ← Component test: producer throughput
      │
      ▼
[AWS Lambda: flight-ops-processor] ← Component test: cold start / warm latency / concurrency
      │
      ▼
[Kafka Topic: flight-processed]    ← E2E test: full pipeline latency (produce → consume)
```

---

## 📁 Repo Structure

```
perf-testing-suite/
├── data/
│   └── flight_schema.py          # Flight ops JSON schema + data generator
├── kafka/
│   └── producer_perf_test.py     # Kafka producer: throughput + burst tests
├── lambda/
│   └── lambda_perf_test.py       # Lambda: cold start, warm latency, concurrency
├── locust/
│   └── locustfile.py             # E2E + component Locust load tests
├── jmeter/
│   └── generate_jmx.py           # Generates JMeter .jmx test plan
├── docker/
│   └── docker-compose.yml        # Local Kafka + LocalStack (Lambda emulation)
├── results/                      # Test output (gitignored)
├── reports/                      # JMeter HTML reports (gitignored)
├── .github/workflows/ci.yml      # GitHub Actions CI pipeline
└── requirements.txt
```

---

## 🚀 Quick Start

### 1. Start Local Infrastructure

```bash
cd docker
docker-compose up -d
```

This starts:
- **Kafka** on `localhost:9092`
- **Kafka UI** on `http://localhost:8090`
- **LocalStack** (Lambda emulation) on `localhost:4566`
- Auto-creates topics: `flight-operations`, `flight-processed`, `flight-dlq`

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 🧪 Running Tests

### Component Test 1 — Kafka Producer Throughput

```bash
# Throughput test (1000 messages)
python kafka/producer_perf_test.py --messages 1000 --test throughput

# Burst test (5 bursts of 500 messages)
python kafka/producer_perf_test.py --burst-size 500 --bursts 5 --test burst

# Both
python kafka/producer_perf_test.py --test all
```

**Metrics captured:** messages/sec, avg/p95/p99/max latency, error rate

---

### Component Test 2 — Lambda Performance

```bash
export LAMBDA_FUNCTION_NAME=flight-ops-processor
export AWS_REGION=us-east-1
# For LocalStack:
export AWS_ENDPOINT_URL=http://localhost:4566

# Warm invocation latency
python lambda/lambda_perf_test.py --test warm --iterations 100

# Concurrent load (20 users, 10 requests each)
python lambda/lambda_perf_test.py --test concurrency --concurrent-users 20

# All tests
python lambda/lambda_perf_test.py --test all
```

**Metrics captured:** cold start latency, warm p50/p95/p99, concurrency throughput

---

### E2E Test — Locust (Kafka → Lambda → Output)

```bash
# Headless mode (CI-friendly)
locust -f locust/locustfile.py \
  --headless --users 50 --spawn-rate 5 \
  --run-time 2m --only-summary \
  FlightOpsProducerUser

# Web UI mode (interactive)
locust -f locust/locustfile.py --host=http://localhost
# Open http://localhost:8089
```

**User types:**
| Class | What it tests |
|---|---|
| `FlightOpsProducerUser` | Kafka producer throughput under concurrent load |
| `FlightOpsPipelineE2EUser` | Full pipeline latency (produce → Lambda → output topic) |

---

### E2E Test — JMeter (Lambda HTTP Endpoint)

```bash
# Generate the JMX file
python jmeter/generate_jmx.py \
  --threads 50 --ramp-up 30 --duration 120 \
  --lambda-endpoint localhost

# Run headless
jmeter -n \
  -t jmeter/flight_ops_perf_test.jmx \
  -l results/results.jtl \
  -e -o reports/jmeter_report

# Open report
open reports/jmeter_report/index.html
```

---

## 📊 Flight Operations Data Schema

All tests use a consistent JSON schema:

```json
{
  "event_id": "uuid",
  "event_type": "flight_update",
  "event_timestamp": "2026-05-03T10:00:00Z",
  "schema_version": "1.0",
  "flight": {
    "flight_number": "BA4821",
    "aircraft_type": "B777",
    "route": {
      "origin": "LHR",
      "destination": "JFK",
      "scheduled_departure": "2026-05-03T12:00:00Z"
    },
    "status": "ON_TIME",
    "delay_minutes": 0,
    "gate": "B22"
  },
  "passengers": { "capacity": 350, "boarded": 340 },
  "metadata": { "source_system": "OPS_CENTER", "priority": "HIGH" }
}
```

Generate sample data:
```bash
python data/flight_schema.py
```

---

## ⚙️ Environment Variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `flight-operations` | Input topic |
| `KAFKA_OUTPUT_TOPIC` | `flight-processed` | Output topic for E2E |
| `LAMBDA_FUNCTION_NAME` | `flight-ops-processor` | Lambda function name |
| `AWS_REGION` | `us-east-1` | AWS region |
| `AWS_ENDPOINT_URL` | _(unset)_ | LocalStack endpoint |
| `E2E_TIMEOUT_SEC` | `10` | E2E pipeline timeout |

---

## 📈 What Each Test Proves

| Test | Tool | What it validates |
|---|---|---|
| Kafka producer throughput | Python | Can the pipeline ingest X msg/sec without loss? |
| Kafka burst load | Python | Does the pipeline recover gracefully from spikes? |
| Lambda warm latency | Python/boto3 | Is processing latency within SLA at steady state? |
| Lambda concurrency | Python/boto3 | Does Lambda scale correctly under parallel load? |
| E2E producer load | Locust | Full pipeline under sustained concurrent producers |
| E2E pipeline latency | Locust | End-to-end time from produce → Lambda → output |
| Lambda HTTP load | JMeter | REST-layer performance with assertions + reporting |

---

## 🛠️ Tech Stack

- **Python 3.11** — test orchestration and data generation
- **Apache Kafka** (via Confluent Docker) — event streaming
- **AWS Lambda** (via LocalStack for local, real AWS for staging)
- **Locust** — distributed load testing with custom Kafka users
- **JMeter** — protocol-level load testing with HTML reporting
- **GitHub Actions** — CI pipeline with Kafka service container
