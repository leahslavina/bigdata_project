import json
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, helpers

KAFKA_TOPIC = 'pubmed-topic'
KAFKA_SERVER = 'localhost:9092'
ELASTICSEARCH_HOST = 'http://localhost:9200'
INDEX_NAME = 'pubmed-index'
JSON_FILE_PATH = 'pubmed_data.json'

def create_index(es_client):
    if not es_client.indices.exists(index=INDEX_NAME):
        mapping = {
            "mappings": {
                "properties": {
                    "pmid": {"type": "keyword"},
                    "question": {"type": "text"},
                    "context": {"type": "text"},
                    "long_answer": {"type": "text"}
                }
            }
        }
        es_client.indices.create(index=INDEX_NAME, body=mapping)
        print(f"Created index: {INDEX_NAME}")

def load_from_json_and_index():
    es = Elasticsearch(ELASTICSEARCH_HOST)
    create_index(es)

    with open(JSON_FILE_PATH, 'r', encoding='utf-8') as file:
        data = json.load(file)

    documents = []
    for doc in data:
        action = {
            "_index": INDEX_NAME,
            "_source": doc
        }
        documents.append(action)

        if len(documents) >= 100:
            helpers.bulk(es, documents)
            documents = []

    if documents:
        helpers.bulk(es, documents)
        
    print("Finished indexing from JSON.")

def consume_from_kafka_and_index():
    es = Elasticsearch(ELASTICSEARCH_HOST)
    create_index(es)

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Listening to Kafka...")
    
    documents = []
    for message in consumer:
        doc = message.value
        
        action = {
            "_index": INDEX_NAME,
            "_source": doc
        }
        documents.append(action)

        if len(documents) >= 100:
            helpers.bulk(es, documents)
            print(f"Indexed {len(documents)} documents.")
            documents = []

if __name__ == "__main__":
    load_from_json_and_index()
    # consume_from_kafka_and_index()