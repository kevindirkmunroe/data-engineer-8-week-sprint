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