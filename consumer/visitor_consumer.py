from kafka import KafkaConsumer
import json


consumer = KafkaConsumer(
    "website-visitors",
    bootstrap_servers="localhost:9092",
    group_id="visitor-tracking-group",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8"))
)


visitors = {}


print("======================================")
print(" Website Visitor Kafka Consumer")
print("======================================")
print("Waiting for visitor events...")
print()


try:

    for message in consumer:

        event = message.value

        visitor_id = event["visitor_id"]

        if visitor_id not in visitors:

            visitors[visitor_id] = {
                "visitor_id": visitor_id,
                "visits": 0,
                "pages": [],
                "device": event["device"],
                "last_visit": event["timestamp"]
            }

        visitors[visitor_id]["visits"] += 1

        visitors[visitor_id]["pages"].append(event["page"])

        visitors[visitor_id]["last_visit"] = event["timestamp"]

        print()
        print("========== NEW VISITOR EVENT ==========")

        print(f"Visitor ID : {event['visitor_id']}")
        print(f"Page       : {event['page']}")
        print(f"Device     : {event['device']}")
        print(f"Timestamp  : {event['timestamp']}")
        print(f"Event      : {event['event']}")

        print()
        print("========== VISITOR SUMMARY ============")

        print(f"Unique Visitors : {len(visitors)}")

        total_visits = sum(
            visitor["visits"]
            for visitor in visitors.values()
        )

        print(f"Total Visits    : {total_visits}")

        print()
        print("Visitor Records:")

        for visitor in visitors.values():

            print(
                f"{visitor['visitor_id']} | "
                f"Visits: {visitor['visits']} | "
                f"Device: {visitor['device']} | "
                f"Last Page: {visitor['pages'][-1]}"
            )

        print("=======================================")


except KeyboardInterrupt:

    print("\nConsumer stopped.")

finally:

    consumer.close()
