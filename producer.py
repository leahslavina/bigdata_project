import json
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_SERVER = "localhost:9092"
KAFKA_TOPIC = "pubmed-topic"
DATA_FILE = Path(__file__).parent / "pubmed_data.json"


def connect_to_kafka(max_attempts=10):
    for attempt in range(1, max_attempts + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                key_serializer=lambda key: key.encode("utf-8"),
                value_serializer=lambda value: json.dumps(
                    value
                ).encode("utf-8"),
                acks="all",
                retries=5,
            )

            print("Connected to Kafka.")
            return producer

        except KafkaError:
            print(
                f"Kafka is not ready. "
                f"Attempt {attempt}/{max_attempts}..."
            )
            time.sleep(3)

    raise RuntimeError("Could not connect to Kafka.")


def stream_records():
    producer = connect_to_kafka()
    sent = 0

    try:
        with DATA_FILE.open("r", encoding="utf-8") as file:
            for line in file:
                if not line.strip():
                    continue

                record = json.loads(line)
                pmid = str(record["pmid"])

                producer.send(
                    KAFKA_TOPIC,
                    key=pmid,
                    value=record,
                )

                sent += 1

                if sent % 100 == 0:
                    producer.flush()
                    print(f"Sent {sent} records...")

                # Simulates records arriving as a stream.
                time.sleep(0.01)

        producer.flush()
        print(
            f"Finished. Sent {sent} records "
            f"to {KAFKA_TOPIC}."
        )

    finally:
        producer.close()


if __name__ == "__main__":
    stream_records()