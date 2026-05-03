"""
AWS Lambda Performance Test
Tests Lambda cold start, warm latency, and concurrent invocation performance
"""

import json
import time
import statistics
import argparse
import os
import sys
import concurrent.futures

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'data'))
from flight_schema import generate_flight_event

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("Install boto3: pip install boto3")
    sys.exit(1)


LAMBDA_FUNCTION_NAME = os.getenv("LAMBDA_FUNCTION_NAME", "flight-ops-processor")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def create_lambda_client():
    return boto3.client(
        "lambda",
        region_name=AWS_REGION,
        endpoint_url=os.getenv("AWS_ENDPOINT_URL")  # for LocalStack
    )


def invoke_lambda(client, payload: dict, invocation_type: str = "RequestResponse") -> dict:
    """Invoke Lambda and return timing + result."""
    start = time.time()
    try:
        response = client.invoke(
            FunctionName=LAMBDA_FUNCTION_NAME,
            InvocationType=invocation_type,
            Payload=json.dumps(payload)
        )
        elapsed_ms = (time.time() - start) * 1000
        status_code = response["StatusCode"]
        response_payload = json.loads(response["Payload"].read())
        return {
            "success": status_code == 200,
            "latency_ms": round(elapsed_ms, 2),
            "status_code": status_code,
            "response": response_payload
        }
    except ClientError as e:
        return {
            "success": False,
            "latency_ms": (time.time() - start) * 1000,
            "error": str(e)
        }


def run_cold_start_test(client, iterations: int = 5) -> dict:
    """Estimate cold start latency (first invocation after gap)."""
    print(f"\n🥶 Cold Start Test — {iterations} iterations (60s gap each)...")
    cold_latencies = []

    for i in range(iterations):
        print(f"  Iteration {i+1}/{iterations} — waiting 65s for cold start...")
        time.sleep(65)
        payload = generate_flight_event("cold_start_test")
        result = invoke_lambda(client, payload)
        if result["success"]:
            cold_latencies.append(result["latency_ms"])
            print(f"  ✅ Cold start latency: {result['latency_ms']}ms")
        else:
            print(f"  ❌ Failed: {result.get('error')}")

    return {
        "test": "lambda_cold_start",
        "iterations": iterations,
        "avg_cold_start_ms": round(statistics.mean(cold_latencies), 2) if cold_latencies else 0,
        "max_cold_start_ms": round(max(cold_latencies), 2) if cold_latencies else 0,
    }


def run_warm_latency_test(client, iterations: int = 100) -> dict:
    """Test warm Lambda invocation latency."""
    print(f"\n🔥 Warm Latency Test — {iterations} sequential invocations...")

    # Warm up
    invoke_lambda(client, generate_flight_event())
    time.sleep(1)

    latencies = []
    errors = 0

    for i in range(iterations):
        result = invoke_lambda(client, generate_flight_event("warm_test"))
        if result["success"]:
            latencies.append(result["latency_ms"])
        else:
            errors += 1

    return {
        "test": "lambda_warm_latency",
        "iterations": iterations,
        "errors": errors,
        "latency_avg_ms": round(statistics.mean(latencies), 2) if latencies else 0,
        "latency_p50_ms": round(sorted(latencies)[int(len(latencies) * 0.50)], 2) if latencies else 0,
        "latency_p95_ms": round(sorted(latencies)[int(len(latencies) * 0.95)], 2) if latencies else 0,
        "latency_p99_ms": round(sorted(latencies)[int(len(latencies) * 0.99)], 2) if latencies else 0,
        "latency_max_ms": round(max(latencies), 2) if latencies else 0,
    }


def run_concurrency_test(client, concurrent_users: int = 20, requests_per_user: int = 10) -> dict:
    """Test Lambda under concurrent load."""
    print(f"\n⚡ Concurrency Test — {concurrent_users} concurrent users, {requests_per_user} requests each...")
    all_latencies = []
    errors = 0

    def worker(_):
        results = []
        for _ in range(requests_per_user):
            r = invoke_lambda(client, generate_flight_event("concurrency_test"))
            results.append(r)
        return results

    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_users) as executor:
        futures = [executor.submit(worker, i) for i in range(concurrent_users)]
        for future in concurrent.futures.as_completed(futures):
            for result in future.result():
                if result["success"]:
                    all_latencies.append(result["latency_ms"])
                else:
                    errors += 1
    total_time = time.time() - start

    return {
        "test": "lambda_concurrency",
        "concurrent_users": concurrent_users,
        "total_requests": concurrent_users * requests_per_user,
        "errors": errors,
        "total_time_sec": round(total_time, 2),
        "throughput_req_per_sec": round((concurrent_users * requests_per_user) / total_time, 2),
        "latency_avg_ms": round(statistics.mean(all_latencies), 2) if all_latencies else 0,
        "latency_p95_ms": round(sorted(all_latencies)[int(len(all_latencies) * 0.95)], 2) if all_latencies else 0,
        "latency_p99_ms": round(sorted(all_latencies)[int(len(all_latencies) * 0.99)], 2) if all_latencies else 0,
    }


def print_results(results: dict):
    print("\n" + "="*55)
    print("📈 RESULTS")
    print("="*55)
    for k, v in results.items():
        print(f"  {k:<40} {v}")
    print("="*55)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lambda Performance Test")
    parser.add_argument("--test", choices=["warm", "concurrency", "cold", "all"], default="all")
    parser.add_argument("--iterations", type=int, default=100)
    parser.add_argument("--concurrent-users", type=int, default=20)
    args = parser.parse_args()

    print(f"🚀 Lambda Perf Test — Function: {LAMBDA_FUNCTION_NAME}, Region: {AWS_REGION}")
    client = create_lambda_client()

    if args.test in ("warm", "all"):
        print_results(run_warm_latency_test(client, args.iterations))

    if args.test in ("concurrency", "all"):
        print_results(run_concurrency_test(client, args.concurrent_users))

    if args.test == "cold":
        print_results(run_cold_start_test(client))

    print("\n✅ Lambda performance tests complete.")
