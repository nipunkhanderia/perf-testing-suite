"""
Kafka Producer Performance Test
Tests throughput and latency of producing flight events to Kafka
"""

import json
import time
import statistics
import argparse
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'data'))
from flight_schema import generate_flight_event, generate_batch

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:
    print("Install kafka-python: pip install kafka-python")
    sys.exit(1)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flight-operations")


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        batch_size=16384,
        linger_ms=5,
        compression_type="gzip"
    )


def run_throughput_test(producer: KafkaProducer, message_count: int = 1000) -> dict:
    """Test how many messages/sec the producer can sustain."""
    print(f"\n📊 Throughput Test — sending {message_count} messages...")
    latencies = []
    errors = 0
    start = time.time()

    for i in range(message_count):
        msg = generate_flight_event()
        msg_start = time.time()
        try:
            future = producer.send(KAFKA_TOPIC, value=msg)
            future.get(timeout=10)
            latencies.append((time.time() - msg_start) * 1000)
        except KafkaError as e:
            errors += 1
            print(f"  ❌ Error on msg {i}: {e}")

    producer.flush()
    total_time = time.time() - start

    return {
        "test": "kafka_producer_throughput",
        "messages_sent": message_count - errors,
        "errors": errors,
        "total_time_sec": round(total_time, 2),
        "throughput_msg_per_sec": round((message_count - errors) / total_time, 2),
        "latency_avg_ms": round(statistics.mean(latencies), 2) if latencies else 0,
        "latency_p95_ms": round(sorted(latencies)[int(len(latencies) * 0.95)], 2) if latencies else 0,
        "latency_p99_ms": round(sorted(latencies)[int(len(latencies) * 0.99)], 2) if latencies else 0,
        "latency_max_ms": round(max(latencies), 2) if latencies else 0,
    }


def run_burst_test(producer: KafkaProducer, burst_size: int = 500, bursts: int = 5) -> dict:
    """Test pipeline behaviour under burst load."""
    print(f"\n⚡ Burst Test — {bursts} bursts of {burst_size} messages...")
    burst_times = []

    for b in range(bursts):
        batch = generate_batch(burst_size)
        start = time.time()
        for msg in batch:
            producer.send(KAFKA_TOPIC, value=msg)
        producer.flush()
        elapsed = time.time() - start
        burst_times.append(elapsed)
        print(f"  Burst {b+1}: {round(elapsed, 2)}s ({round(burst_size/elapsed, 0)} msg/s)")
        time.sleep(1)

    return {
        "test": "kafka_producer_burst",
        "burst_size": burst_size,
        "burst_count": bursts,
        "avg_burst_time_sec": round(statistics.mean(burst_times), 2),
        "avg_throughput_msg_per_sec": round(burst_size / statistics.mean(burst_times), 2),
    }


def print_results(results: dict):
    print("\n" + "="*50)
    print("📈 RESULTS")
    print("="*50)
    for k, v in results.items():
        print(f"  {k:<35} {v}")
    print("="*50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Producer Performance Test")
    parser.add_argument("--messages", type=int, default=1000, help="Number of messages for throughput test")
    parser.add_argument("--burst-size", type=int, default=500, help="Messages per burst")
    parser.add_argument("--bursts", type=int, default=3, help="Number of bursts")
    parser.add_argument("--test", choices=["throughput", "burst", "all"], default="all")
    args = parser.parse_args()

    print(f"🚀 Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}, topic: {KAFKA_TOPIC}")
    producer = create_producer()

    if args.test in ("throughput", "all"):
        results = run_throughput_test(producer, args.messages)
        print_results(results)

    if args.test in ("burst", "all"):
        results = run_burst_test(producer, args.burst_size, args.bursts)
        print_results(results)

    producer.close()
    print("\n✅ Kafka producer tests complete.")
