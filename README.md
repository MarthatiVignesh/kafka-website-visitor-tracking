# Kafka Website Visitor Tracking

A real-time website visitor tracking project using **Apache Kafka** and **Python**.

## Project Overview

This project demonstrates how website visitor events can be produced to a Kafka topic and consumed in real time to maintain visitor tracking information.

### Architecture

```
Website Visitor Events
        |
        v
Python Kafka Producer
        |
        v
Kafka Topic: website-visitors
        |
        v
Python Kafka Consumer
        |
        v
Visitor Tracking Data
```

## Project Structure

```
kafka-website-visitor-tracking/
├── consumer/
│   └── visitor_consumer.py
├── data/
├── docker-compose.yml
├── producer/
│   └── visitor_producer.py
└── .gitignore
```

## Technologies Used

- Python
- Apache Kafka 3.8.0
- Docker
- Docker Compose
- kafka-python
- JSON

## Kafka Configuration

- Kafka container: `website-kafka`
- Kafka topic: `website-visitors`
- Partitions: 3
- Replication factor: 1

## Input

The producer generates website visitor events in JSON format.

Example input event:

```json
{
  "visitor_id": "VIS-1001",
  "page": "/products",
  "device": "Chrome",
  "timestamp": "2026-09-22T10:30:15",
  "event": "page_view"
}
```

The producer generates visitor IDs, pages, devices, timestamps, and page-view events automatically.

### Possible Pages

```
/home
/about
/products
/contact
/services
```

### Possible Devices

```
Chrome
Firefox
Edge
Safari
Mobile
```

## Output

The consumer reads events from the `website-visitors` Kafka topic and maintains visitor information.

Example output:

```
Visitor ID: VIS-1001
Visit Count: 3
Pages Visited: ['/home', '/products', '/contact']
Device: Chrome
Last Visit: 2026-09-22T10:30:15
```

The consumer can track:

- Unique visitors
- Visit count
- Pages visited
- Device used
- Last visit time

## How to Run

### 1. Start Kafka

From the project directory:

```bash
docker compose up -d
```

### 2. Activate Python Virtual Environment

```source venv/bin/activate```

### 3. Install Dependency

```bash
pip install kafka-python
```

### 4. Run the Producer

Open a terminal and run:

```bash
python producer/visitor_producer.py
```

The producer sends a new visitor event every few seconds.

### 5. Run the Consumer

Open another terminal and run:

```bash
python consumer/visitor_consumer.py
```

The consumer receives and processes visitor events in real time.

## Kafka Topic Verification

The topic can also be checked using the Kafka console consumer:

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic website-visitors --from-beginning
```

## Sample Flow

```
Input Event
   |
   v
Producer
   |
   v
Kafka: website-visitors
   |
   v
Consumer
   |
   v
Visitor Statistics
```

## Result

The project successfully demonstrates real-time website visitor tracking using Kafka. Visitor events are continuously produced, stored in the Kafka topic, consumed by the Python consumer, and used to maintain visitor statistics.

## Author

MarthatiVignesh
