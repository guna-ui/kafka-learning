# 🚀 Apache Kafka — Producer & Consumer with Python

A hands-on project demonstrating real-time event streaming using **Apache Kafka** with Python. Built to understand the fundamentals of distributed messaging systems.

## 📌 What This Project Does

- **Producer** sends simulated user login events to a Kafka topic every second
- **Consumer** listens to the topic in real-time and processes incoming events
- Demonstrates key Kafka concepts: topics, partitions, offsets, serialization, and consumer groups

## 🏗️ Architecture

```
┌──────────────┐       ┌──────────────┐       ┌──────────────┐
│   Producer   │──────▶│    Kafka      │──────▶│   Consumer   │
│ (producer.py)│       │   Broker      │       │(consumer.py) │
│              │       │              │       │              │
│ Sends JSON   │       │ Topic:       │       │ Reads JSON   │
│ events       │       │ "user_id"    │       │ events       │
└──────────────┘       └──────┬───────┘       └──────────────┘
                              │
                       ┌──────┴───────┐
                       │  ZooKeeper   │
                       │ (Cluster     │
                       │  Manager)    │
                       └──────────────┘
```

## 🧠 Key Concepts Learned

| Concept | What I Learned |
|---|---|
| **Broker** | The Kafka server that stores and serves messages |
| **Topic** | A named channel/category where messages are published (like a folder) |
| **Partition** | Topics are split into partitions for parallel processing |
| **Offset** | A unique, incrementing ID for each message within a partition (like a page number) |
| **Producer** | Sends (publishes) messages to a topic |
| **Consumer** | Reads (subscribes to) messages from a topic |
| **Consumer Group** | A group ID that lets Kafka track what a consumer has already read |
| **Serialization** | Converting Python objects to bytes (JSON → bytes) for Kafka to store |
| **ZooKeeper** | External service that manages Kafka cluster metadata (being replaced by KRaft) |

## 🛠️ Tech Stack

- **Python 3.x** — Application logic
- **kafka-python** — Python client for Apache Kafka
- **Docker & Docker Compose** — Containerized Kafka + ZooKeeper setup
- **Confluent Kafka Images** — Production-grade Docker images (v7.6.12)

## 📁 Project Structure

```
kafka-learning/
├── docker-compose.yml   # Kafka + ZooKeeper container setup
├── producer.py          # Sends events to Kafka topic
├── consumer.py          # Reads events from Kafka topic
├── requirements.txt     # Python dependencies
└── README.md            # You are here!
```

## ⚡ Quick Start

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- Python 3.x installed

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/kafka-learning.git
cd kafka-learning
```

### 2. Start Kafka and ZooKeeper
```bash
docker-compose up -d
```

### 3. Install Python dependencies
```bash
pip install kafka-python
```

### 4. Run the Producer (Terminal 1)
```bash
python producer.py
```
You should see messages being sent every second:
```
sent message0:{'user_id': 'user_id:0', 'action': 'login', ...}
sent message1:{'user_id': 'user_id:1', 'action': 'login', ...}
```

### 5. Run the Consumer (Terminal 2)
```bash
python consumer.py
```
You should see messages being received in real-time:
```
Received: Key=None, Value={'user_id': 'user_id:0', ...}, Partition=0, Offset=0
```

### 6. Stop everything
```bash
# Stop consumer/producer with Ctrl+C
# Stop Kafka containers
docker-compose down
```

## 💡 Lessons & Gotchas

1. **Offsets never reset** — Even after restarting producer/consumer, offsets keep incrementing. They are permanent position markers within a partition.

2. **Consumer Group ID matters** — Without a `group_id`, the consumer reads from the beginning every time. With one, Kafka remembers where you left off.

3. **`auto_offset_reset='earliest'` vs `'latest'`** — `earliest` reads all historical messages; `latest` only reads new ones. This setting is only used when no saved offset exists for the consumer group.

4. **Version matching** — ZooKeeper and Kafka Docker images should use the same version (e.g., both `7.6.12`) to avoid compatibility issues.

5. **Environment variables are case-sensitive** — `KAFKA_BROKER_ID` works, `kafka_broker_id` does not.


