from kafka import KafkaProducer
import json
import time
from datetime import datetime
import random


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


pages = [
    "/home",
    "/about",
    "/products",
    "/contact",
    "/services"
]


devices = [
    "Chrome",
    "Firefox",
    "Edge",
    "Safari",
    "Mobile"
]


visitor_ids = []


def create_event(visitor_id):

    return {
        "visitor_id": visitor_id,
        "page": random.choice(pages),
        "device": random.choice(devices),
        "timestamp": datetime.now().isoformat(),
        "event": "PAGE_VISIT"
    }


print("======================================")
print(" Website Visitor Kafka Producer")
print("======================================")
print("Sending visitor events...")
print("Press CTRL+C to stop")
print()


visitor_number = 1001


try:

    while True:

        if visitor_ids and random.random() < 0.7:

            visitor_id = random.choice(visitor_ids)

        else:

            visitor_id = f"VIS-{visitor_number}"

            visitor_ids.append(visitor_id)

            visitor_number += 1


        event = create_event(visitor_id)


        producer.send(
            "website-visitors",
            key=visitor_id.encode("utf-8"),
            value=event
        )

        producer.flush()


        print("Event sent:")
        print(json.dumps(event, indent=2))
        print("--------------------------------------")


        time.sleep(3)


except KeyboardInterrupt:

    print("\nProducer stopped.")


finally:

    producer.close()
