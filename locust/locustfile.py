"""
Locust E2E Performance Test
End-to-end load test: Kafka → Lambda pipeline for flight operations data

Run with:
    locust -f locustfile.py --host=http://localhost:8080
    locust -f locustfile.py --headless -u 50 -r 5 --run-time 2m
"""

import json
import os
import sys
import time
import random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'data'))
from flight_schema import generate_flight_event, generate_batch

try:
    from locust import User, task, between, events
    from kafka import KafkaProducer, KafkaConsumer
    from kafka.errors import KafkaError
except ImportError:
    print("Install dependencies: pip install locust kafka-python")
    sys.exit(1)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "flight-operations")
KAFKA_OUTPUT_TOPIC = os.getenv("KAFKA_OUTPUT_TOPIC", "flight-processed")
E2E_TIMEOUT_SEC = int(os.getenv("E2E_TIMEOUT_SEC", "10"))


# ─── Kafka Producer User ────────────────────────────────────────────────────

class FlightOpsProducerUser(User):
    """
    Simulates flight operations system pushing events to Kafka.
    Tests Kafka ingestion throughput under concurrent load.
    """
    wait_time = between(0.1, 0.5)
    producer = None

    def on_start(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=1,
            linger_ms=5
        )

    def on_stop(self):
        if self.producer:
            self.producer.close()

    @task(3)
    def produce_single_flight_update(self):
        """Single flight status update — most common event."""
        msg = generate_flight_event("flight_update")
        self._produce_and_record(msg, "produce_flight_update")

    @task(1)
    def produce_flight_departure(self):
        """Flight departure event."""
        msg = generate_flight_event("flight_departure")
        self._produce_and_record(msg, "produce_flight_departure")

    @task(1)
    def produce_batch_updates(self):
        """Burst of 10 updates — simulates bulk ops center sync."""
        batch = generate_batch(10)
        start = time.time()
        errors = 0
        try:
            for msg in batch:
                self.producer.send(KAFKA_TOPIC, value=msg)
            self.producer.flush()
            elapsed = int((time.time() - start) * 1000)
            events.request.fire(
                request_type="kafka",
                name="produce_batch_10",
                response_time=elapsed,
                response_length=len(batch),
                exception=None,
                context={}
            )
        except KafkaError as e:
            elapsed = int((time.time() - start) * 1000)
            events.request.fire(
                request_type="kafka",
                name="produce_batch_10",
                response_time=elapsed,
                response_length=0,
                exception=e,
                context={}
            )

    def _produce_and_record(self, msg: dict, name: str):
        start = time.time()
        try:
            future = self.producer.send(KAFKA_TOPIC, value=msg)
            future.get(timeout=5)
            elapsed = int((time.time() - start) * 1000)
            events.request.fire(
                request_type="kafka",
                name=name,
                response_time=elapsed,
                response_length=len(json.dumps(msg)),
                exception=None,
                context={}
            )
        except KafkaError as e:
            elapsed = int((time.time() - start) * 1000)
            events.request.fire(
                request_type="kafka",
                name=name,
                response_time=elapsed,
                response_length=0,
                exception=e,
                context={}
            )


# ─── E2E Pipeline User ───────────────────────────────────────────────────────

class FlightOpsPipelineE2EUser(User):
    """
    Full E2E test: produce to Kafka, wait for Lambda to process,
    consume from output topic and verify latency.
    """
    wait_time = between(1, 3)
    producer = None
    consumer = None

    def on_start(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all"
        )
        self.consumer = KafkaConsumer(
            KAFKA_OUTPUT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            consumer_timeout_ms=E2E_TIMEOUT_SEC * 1000,
            group_id=f"perf-test-{random.randint(1000, 9999)}"
        )

    def on_stop(self):
        if self.producer:
            self.producer.close()
        if self.consumer:
            self.consumer.close()

    @task
    def e2e_flight_event_pipeline(self):
        """
        Produce a flight event and measure time until it appears
        in the processed output topic (full pipeline latency).
        """
        msg = generate_flight_event("e2e_test")
        event_id = msg["event_id"]
        start = time.time()

        try:
            # Step 1: Produce to Kafka
            self.producer.send(KAFKA_TOPIC, value=msg)
            self.producer.flush()

            # Step 2: Poll output topic for processed message
            found = False
            deadline = time.time() + E2E_TIMEOUT_SEC
            while time.time() < deadline:
                records = self.consumer.poll(timeout_ms=500)
                for _, msgs in records.items():
                    for record in msgs:
                        if isinstance(record.value, dict) and record.value.get("event_id") == event_id:
                            found = True
                            break
                if found:
                    break

            elapsed = int((time.time() - start) * 1000)

            if found:
                events.request.fire(
                    request_type="e2e_pipeline",
                    name="kafka_lambda_e2e_latency",
                    response_time=elapsed,
                    response_length=1,
                    exception=None,
                    context={}
                )
            else:
                events.request.fire(
                    request_type="e2e_pipeline",
                    name="kafka_lambda_e2e_latency",
                    response_time=elapsed,
                    response_length=0,
                    exception=Exception(f"Timeout: event {event_id} not found in output topic"),
                    context={}
                )
        except Exception as e:
            elapsed = int((time.time() - start) * 1000)
            events.request.fire(
                request_type="e2e_pipeline",
                name="kafka_lambda_e2e_latency",
                response_time=elapsed,
                response_length=0,
                exception=e,
                context={}
            )
