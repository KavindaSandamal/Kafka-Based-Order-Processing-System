# ☕ Kafka Orders Processing System

## 🚀 Overview

This project simulates an **orders stream** using **Apache Kafka** to demonstrate resilient message processing.

A **Python Producer** sends synthetic order messages, encoded in **Avro format**, to the main Kafka topic. A **Python Consumer** processes these orders, calculates a running average price, and implements sophisticated error handling:

- **Retry Logic:** Handles transient/temporary failures (simulated 10% chance).
- **Dead Letter Queue (DLQ):** Redirects messages that fail permanently (e.g., invalid data) or after exhausting all retries.

---

## 🗄️ Topics

- `orders` – main order stream
- `orders-dlq` – dead letter queue for permanently failed messages

---

## ⚙️ Prerequisites

To run this project, you need the following installed:

1.  **Docker & Docker Compose:** To run the Kafka and Zookeeper services.
2.  **Python 3.x:** To run the producer and consumer scripts.
3.  **Python Dependencies:** Install the required libraries for both producer and consumer:
    ```bash
    pip install confluent-kafka fastavro requests
    ```

---

## ▶️ How to Run the Project (Step-by-Step)

Follow these steps exactly in order. You will need **four separate terminal windows** open simultaneously for the Python scripts and one for Docker.

### Step 1: Start Kafka and Zookeeper

Start the Kafka broker and Zookeeper using Docker Compose.

Run this command in your project's root directory:

```bash
docker compose up -d
```

### Step 2: Start the DLQ Consumer

Open a new terminal.

```bash
cd consumer
python dlq_consumer.py
```

### Step 3: Start the Main Consumer

Open a second new terminal.

```bash
cd consumer
python consumer.py
```

### Step 4: Start the Producer

Open a third new terminal.

```bash
cd producer
python producer.py
```

### Step 5: Clean Up

When you are finished, stop all three Python scripts using `Ctrl+C` in their respective terminals, then stop and remove the Docker containers.

In your Docker terminal:

```bash
docker compose down
```
