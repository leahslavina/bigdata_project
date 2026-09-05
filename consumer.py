import json
import time

from elasticsearch import Elasticsearch, helpers
from kafka import KafkaConsumer
from kafka.errors import KafkaError

KAFKA_TOPIC = "pubmed-topic"
KAFKA_SERVER = "localhost:9092"
ELASTICSEARCH_HOST = "http://localhost:9200"
INDEX_NAME = "pubmed-index"
BATCH_SIZE = 100

def transform_record(record):
    """Clean the Kafka record and add fields useful for search and analysis."""
    doc = dict(record)

    question = str(doc.get("question") or "").strip()
    context = doc.get("context") or ""
    long_answer = doc.get("long_answer") or ""

    if isinstance(context, list):
        context = " ".join(str(part) for part in context)
    else:
        context = str(context)

    if isinstance(long_answer, list):
        long_answer = " ".join(str(part) for part in long_answer)
    else:
        long_answer = str(long_answer)

    doc["question"] = question
    doc["context"] = context.strip()
    doc["long_answer"] = long_answer.strip()
    doc["final_decision"] = str(
        doc.get("final_decision") or ""
    ).strip().lower()

    doc["context_word_count"] = len(doc["context"].split())
    doc["has_long_answer"] = bool(doc["long_answer"])
    doc["search_text"] = " ".join(
        part for part in [
            doc["question"],
            doc["context"],
            doc["long_answer"],
        ]
        if part
    )

    return doc


def connect_to_elasticsearch(max_attempts=20):
    for attempt in range(1, max_attempts + 1):
        es = Elasticsearch(ELASTICSEARCH_HOST)

        if es.ping():
            print("Connected to Elasticsearch.")
            return es

        es.close()
        print(
            f"Elasticsearch is not ready. "
            f"Attempt {attempt}/{max_attempts}..."
        )
        time.sleep(3)

    raise RuntimeError(
        "Could not connect to Elasticsearch."
    )


def create_index(es):
    if es.indices.exists(index=INDEX_NAME):
        print(f"Using existing index: {INDEX_NAME}")
        return

    es.indices.create(
        index=INDEX_NAME,
        settings={
            "number_of_replicas": 0
        },
        mappings={
            "properties": {
                "pmid": {
                    "type": "keyword"
                },
                "question": {
                    "type": "text"
                },
                "context": {
                    "type": "text"
                },
                "labels": {
                    "type": "keyword"
                },
                "long_answer": {
                    "type": "text"
                },
                "final_decision": {
                    "type": "keyword"
                },
		"context_word_count": {"type": "integer"},
		"has_long_answer": {"type": "boolean"},
		"search_text": {"type": "text"},
            }
        },
    )

    print(f"Created index: {INDEX_NAME}")


def connect_to_kafka(max_attempts=20):
    for attempt in range(1, max_attempts + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_SERVER],
                group_id="pubmed-elasticsearch-indexer",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(
                    value.decode("utf-8")
                ),
                consumer_timeout_ms=120000,
            )

            print("Connected to Kafka.")
            return consumer

        except KafkaError:
            print(
                f"Kafka is not ready. "
                f"Attempt {attempt}/{max_attempts}..."
            )
            time.sleep(3)

    raise RuntimeError("Could not connect to Kafka.")


def index_batch(es, records):
    actions = [
        {
            "_index": INDEX_NAME,
            "_id": str(record["pmid"]),
            "_source": record,
        }
        for record in records
    ]

    helpers.bulk(es, actions)
    es.indices.refresh(index=INDEX_NAME)


def consume_and_index():
    es = connect_to_elasticsearch()
    create_index(es)
    consumer = connect_to_kafka()

    print(f"Listening to Kafka topic: {KAFKA_TOPIC}")

    batch = []
    total = 0

    try:
        for message in consumer:
            batch.append(transform_record(message.value))

            if len(batch) >= BATCH_SIZE:
                index_batch(es, batch)
                total += len(batch)
                print(f"Indexed {total} records...")
                batch = []

        if batch:
            index_batch(es, batch)
            total += len(batch)
            print(f"Indexed {total} records...")

        final_count = es.count(
            index=INDEX_NAME
        )["count"]

        print(f"Finished. Consumed {total} records.")
        print(
            f"Documents in {INDEX_NAME}: "
            f"{final_count}"
        )

    finally:
        consumer.close()
        es.close()


if __name__ == "__main__":
    consume_and_index()