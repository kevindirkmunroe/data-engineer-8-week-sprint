---
layout: post
title: "Week 2: Stepping Up to Real-Time Streaming with Kafka and Python"
date: 2026-10-23
categories: [data-engineering, kafka, python, streaming]
---

Last week, we laid down a solid foundation by spinning up a containerized PostgreSQL instance via Docker and verifying our connection inside TablePlus. A local database is a great anchor, but modern data engineering rarely stops at static storage—applications live and breathe on real-time event streams.

Welcome to **Week 2** of the data engineering sprint. 

In this chapter, we are bridging the gap between static storage and dynamic streaming. Moving from batch-inserts to live event processing means dealing with asynchronous data flow, partition management, and reliable consumer sinks. 

Here is what we are tackling this week:
1. **Expanding the Docker Stack:** Adding Apache Kafka and Zookeeper right alongside our existing Postgres container.
2. **The Producer Side:** Writing a Python script using `confluent-kafka` to generate and stream synthetic event payloads.
3. **The Consumer Sink:** Pulling those messages off the Kafka topic and batch-writing them directly into our `engineering_db`.

Let's dive into the architecture and get our streams flowing!

### Quick Note about `psql`

If we were able to connect to Postgres DB with TablePlus, why take the time to get `psql` up and running?
I figured this would come up again. Gemini says:

> While a GUI tool like TablePlus gives us an instant, user-friendly window into our containers, mastering the psql command-line interface is critical for any production-grade data pipeline. GUIs abstract away the underlying engine, but psql gives us direct, low-level control over connection parameters, transaction isolation levels, bulk data copy operations, and automated shell scripting—skills that become indispensable when orchestrating and debugging automated data loads from streaming sinks like Kafka.

Fair enough. So for my use case where I *don't* want to update Xcode, I can download postgres binaries from here: [EDB Posgres](https://www.enterprisedb.com/download-postgresql-binaries). Use the `psql` binary to connect to port `5432` then run `\l`:

```commandline
postgres=# \l
                                                   List of databases
      Name      |  Owner   | Encoding |  Collate   |   Ctype    | ICU Locale | Locale Provider |   Access privileges   
----------------+----------+----------+------------+------------+------------+-----------------+-----------------------
 engineering_db | postgres | UTF8     | en_US.utf8 | en_US.utf8 |            | libc            | 
 postgres       | postgres | UTF8     | en_US.utf8 | en_US.utf8 |            | libc            | 
 template0      | postgres | UTF8     | en_US.utf8 | en_US.utf8 |            | libc            | =c/postgres          +
                |          |          |            |            |            |                 | postgres=CTc/postgres
 template1      | postgres | UTF8     | en_US.utf8 | en_US.utf8 |            | libc            | =c/postgres          +
                |          |          |            |            |            |                 | postgres=CTc/postgres
(4 rows)
```

## Configure Kafka with Zookeeper

<img src="{{ '/assets/images/queue-i-love-lucy.png' | relative_url }}" width="300" alt="I Love Lucy queue">

Apache Kafka is a distributed commit log pressed into service as a queue for real-time event streaming. Zookeeper is a distributed coordination service that provides core infrastructure to help clustered applications (Kafka, HBase etc) manage shared data and synchronization. I updated docker-compose.yml file to this:
```yaml
services:
  postgres:
    image: postgres:15-alpine
    container_name: de_postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgrespassword
      POSTGRES_DB: engineering_db
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - de-network

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: de_zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - de-network

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: de_kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT_INTERNAL
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    networks:
      - de-network

volumes:
  postgres_data:

networks:
  de-network:
    driver: bridge
```

Let's break down the `zookeeper` configuration. I'm not getting bogged down in the details of every parameter r/n.
this is just a reference for features we'll need to tweak later:
<details markdown="block">
<summary>Zookeeper configuration</summary>

| Key                     | Value                             | Meaning                                                                                                                             |
| ----------------------- | --------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `zookeeper`             | —                                 | Defines the ZooKeeper service in Docker Compose. ZooKeeper coordinates distributed systems, including older Kafka deployments.      |
| `image`                 | `confluentinc/cp-zookeeper:7.5.0` | Docker image for Confluent Platform's ZooKeeper, version 7.5.0.                                                                     |
| `container_name`        | `de_zookeeper`                    | Assigns a fixed name to the Docker container.                                                                                       |
| `environment`           | —                                 | Defines environment variables used to configure ZooKeeper.                                                                          |
| `ZOOKEEPER_CLIENT_PORT` | `2181`                            | Port on which ZooKeeper listens for client connections, such as connections from Kafka brokers.                                     |
| `ZOOKEEPER_TICK_TIME`   | `2000`                            | ZooKeeper's basic timing interval in milliseconds (2 seconds), used for heartbeats, session timeouts, and synchronization.          |
| `networks`              | —                                 | Specifies which Docker networks the container joins.                                                                                |
| `- de-network`          | `de-network`                      | Connects ZooKeeper to the Docker network named `de-network`, allowing other containers on that network to reach it by service name. |

</details>

Notes:

- Port 2181 is accessible to other containers on `de-network`, but isn't exposed to the host machine because no `ports:` mapping is defined.
- ZooKeeper is used by older Kafka architectures. Modern Kafka supports KRaft mode, which eliminates the need for ZooKeeper.
- Other containers on the same network can typically connect using `zookeeper:2181`, where `zookeeper` is the Docker Compose service name.

Now lets do the same for `kafka`:

<details markdown="block">
<summary>Kafka configuration</summary>

| Key                                      | Value                                                         | Meaning                                                                                                                                              |
| ---------------------------------------- | ------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `kafka`                                  | —                                                             | Defines the Kafka broker service in Docker Compose. Kafka is a distributed event-streaming platform for publishing, storing, and consuming messages. |
| `image`                                  | `confluentinc/cp-kafka:7.5.0`                                 | Docker image for Confluent Platform's Apache Kafka, version 7.5.0.                                                                                   |
| `container_name`                         | `de_kafka`                                                    | Assigns a fixed name to the Docker container.                                                                                                        |
| `depends_on`                             | `zookeeper`                                                   | Starts the ZooKeeper container before Kafka. Does not guarantee ZooKeeper is fully ready to accept connections.                                      |
| `ports`                                  | `"9092:9092"`                                                 | Maps host port `9092` to container port `9092`, allowing Kafka clients on the host machine to connect.                                               |
| `environment`                            | —                                                             | Defines environment variables used to configure the Kafka broker.                                                                                    |
| `KAFKA_BROKER_ID`                        | `1`                                                           | Unique numeric identifier for this Kafka broker within the cluster.                                                                                  |
| `KAFKA_ZOOKEEPER_CONNECT`                | `zookeeper:2181`                                              | ZooKeeper connection address. Uses the Docker Compose service name `zookeeper` and port `2181`.                                                      |
| `KAFKA_ADVERTISED_LISTENERS`             | `PLAINTEXT://localhost:9092,PLAINTEXT_INTERNAL://kafka:29092` | Addresses Kafka advertises to clients. `localhost:9092` is for host-machine clients; `kafka:29092` is for clients running inside the Docker network. |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`   | `PLAINTEXT:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT`            | Maps listener names to security protocols. Both listeners use unencrypted PLAINTEXT communication.                                                   |
| `KAFKA_INTER_BROKER_LISTENER_NAME`       | `PLAINTEXT_INTERNAL`                                          | Specifies which listener Kafka brokers use to communicate with one another.                                                                          |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | `1`                                                           | Sets the replication factor for Kafka's internal consumer-offsets topic to 1. Suitable for a single-broker development environment.                  |
| `networks`                               | —                                                             | Specifies which Docker networks the Kafka container joins.                                                                                           |
| `- de-network`                           | `de-network`                                                  | Connects Kafka to the same Docker network as ZooKeeper and other data-engineering services.                                                          |


</details>

Notes:

- Port 9092 is intended for Kafka clients running on your host machine, such as a local Python producer or consumer.
- Port 29092 is intended for other Docker containers, such as Kafka Connect, Flink, or a containerized Python application.
- Replication factor 1 is appropriate for local development but provides no broker-level redundancy. Production clusters typically use multiple brokers and higher replication factors.
- PLAINTEXT means communication is neither encrypted nor authenticated. Suitable for isolated local development, but not recommended for production.
- ZooKeeper dependency: This configuration uses ZooKeeper-based Kafka rather than the newer KRaft architecture.

Ok, enough setup- let's produce a data stream and point it at Kafka. First create a virtual environment:

```commandline
   $ python3 -m venv .venv
   $ source .venv/bin/activate
   
   # install kafka client...
   $ pip install confluent_kafka
```
We have a script that runs forever, sending messages to `localhost:9092`. I can see the events in the
`kafka-ui` page running on `localhost:8080`..

<details markdown="block">
<summary>produce-events.py</summary>

```python
import json
import time
import random
from confluent_kafka import Producer

# Configuration for your local Kafka broker spun up via Docker
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}

# Create Producer instance
producer = Producer(conf)
topic_name = 'engineering_events'


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f'✅ Delivered message to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}')


def generate_mock_event():
    """Generates a random streaming event payload."""
    users = ['alice', 'bob', 'charlie', 'diana']
    actions = ['login', 'click_stream', 'purchase', 'logout']

    return {
        "event_id": random.randint(10000, 99999),
        "user": random.choice(users),
        "action": random.choice(actions),
        "timestamp": time.time(),
        "status_code": random.choice([200, 201, 400, 500])
    }


if __name__ == '__main__':
    print(f"🚀 Starting Kafka producer. Streaming events to topic: '{topic_name}'...")

    try:
        while True:
            # Generate event data and convert to JSON string
            event_data = generate_mock_event()
            payload = json.dumps(event_data)

            # Trigger any available delivery report callbacks from previous produce() calls
            producer.poll(0)

            # Asynchronously send the message to Kafka
            producer.produce(
                topic_name,
                key=str(event_data['user']),
                value=payload.encode('utf-8'),
                callback=delivery_report
            )

            # Wait 1 second before generating the next event
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\n🛑 Flushing remaining messages and shutting down producer...")
        # Wait up to 10 seconds for outstanding messages to be delivered
        producer.flush(10.0)
```
</details>

So why can't I see the producer event in Docker Desktop logs?

When you run a Kafka producer script locally (like the Python script we just wrote) on your host machine, the events are being sent directly to the Kafka broker running inside Docker (`localhost:9092`), but **they won't show up in Docker Desktop's container logs**.

Here is why that happens and how Kafka actually handles data storage:

### 1. Kafka is a Storage System, Not a Console Logger

Unlike a traditional web server (like Node.js, Flask, or Express) that prints incoming HTTP requests or `console.log()` statements to `stdout` (which Docker captures as container logs), Apache Kafka treats data as a **durable distributed commit log**.

* When your Python script produces an event, Kafka immediately writes that raw binary payload to disk inside its internal log segments (under `/var/lib/kafka/data` or equivalent data directories inside the container).
* It does **not** echo incoming message payloads to standard output or standard error unless explicitly configured via custom logging interceptors.

### 2. What *Does* Show Up in Docker Logs?

If you open the logs for `de_kafka` in Docker Desktop, you will only see:

* **Broker lifecycle events:** Startup sequences, ZooKeeper cluster handshakes, port binding confirmations, and partition leader elections.
* **System warnings/errors:** Connection drops, out-of-memory warnings, or broker rebalancing notices.

---

### How to Actually Verify Your Events Are Arriving

Since container logs won't show the event payloads, you have three great ways to inspect what your producer is sending:

1. **The Python Delivery Callback:**
If you used the `delivery_report` callback in your Python script (`producer.produce(..., callback=delivery_report)`), your terminal running Python will print success messages with partition numbers and offsets as soon as the broker acknowledges them:
```text
✅ Delivered message to engineering_events [partition 0] at offset 42

```


2. **The Kafka Console Consumer (CLI):**
You can spin up an ephemeral container command to read directly from the topic's commit log and print the events to your terminal:
```bash
docker exec -it de_kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic engineering_events --from-beginning

```


3. **Check Topic Metadata:**
To verify that your topic exists and is actively receiving messages and tracking offsets, you can describe the topic via the CLI:
```bash
docker exec -it de_kafka kafka-topics --describe --topic engineering_events --bootstrap-server localhost:9092

```



---
