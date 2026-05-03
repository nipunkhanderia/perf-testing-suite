"""
Flight Operations Data Schema & Generator
Generates realistic flight operations JSON messages for performance testing
"""

import json
import random
import uuid
from datetime import datetime, timedelta


AIRLINES = ["BA", "EK", "LH", "AA", "DL", "UA", "QR", "SQ", "AF", "KL"]
AIRPORTS = ["LHR", "JFK", "DXB", "FRA", "CDG", "SIN", "LAX", "ORD", "AMS", "DOH"]
STATUSES = ["ON_TIME", "DELAYED", "CANCELLED", "BOARDING", "DEPARTED", "LANDED"]
AIRCRAFT_TYPES = ["B737", "B777", "B787", "A320", "A330", "A350", "A380"]


def generate_flight_event(event_type: str = "flight_update") -> dict:
    """Generate a single flight operations event matching the pipeline schema."""
    departure_time = datetime.utcnow() + timedelta(minutes=random.randint(-60, 300))
    arrival_time = departure_time + timedelta(hours=random.randint(1, 14))

    origin = random.choice(AIRPORTS)
    destination = random.choice([a for a in AIRPORTS if a != origin])

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": datetime.utcnow().isoformat() + "Z",
        "schema_version": "1.0",
        "flight": {
            "flight_number": f"{random.choice(AIRLINES)}{random.randint(100, 9999)}",
            "aircraft_type": random.choice(AIRCRAFT_TYPES),
            "airline_code": random.choice(AIRLINES),
            "route": {
                "origin": origin,
                "destination": destination,
                "scheduled_departure": departure_time.isoformat() + "Z",
                "scheduled_arrival": arrival_time.isoformat() + "Z",
                "actual_departure": (departure_time + timedelta(minutes=random.randint(-5, 60))).isoformat() + "Z",
                "actual_arrival": None
            },
            "status": random.choice(STATUSES),
            "delay_minutes": random.randint(0, 180),
            "gate": f"{random.choice('ABCDEF')}{random.randint(1, 50)}",
            "terminal": str(random.randint(1, 5))
        },
        "passengers": {
            "capacity": random.randint(100, 500),
            "boarded": random.randint(80, 490),
            "checked_in": random.randint(90, 500)
        },
        "crew": {
            "captain": f"CPT{random.randint(1000, 9999)}",
            "first_officer": f"FO{random.randint(1000, 9999)}",
            "cabin_crew_count": random.randint(4, 20)
        },
        "metadata": {
            "source_system": "OPS_CENTER",
            "priority": random.choice(["LOW", "MEDIUM", "HIGH"]),
            "region": random.choice(["EU", "US", "APAC", "ME"])
        }
    }


def generate_batch(size: int = 100, event_type: str = "flight_update") -> list:
    """Generate a batch of flight events."""
    return [generate_flight_event(event_type) for _ in range(size)]


if __name__ == "__main__":
    # Preview sample message
    sample = generate_flight_event()
    print(json.dumps(sample, indent=2))
