import json
from pathlib import Path

from elasticsearch import Elasticsearch

INDEX = "pubmed-dev-index"
DATA_FILE = Path(__file__).parent / "pubmed_data.json"


def main():
    # The dataset contains one JSON record per line.
    records = []
    with DATA_FILE.open(encoding="utf-8") as file:
        for line in file:
            if line.strip():
                records.append(json.loads(line))
            if len(records) == 10:
                break

    with Elasticsearch("http://localhost:9200") as es:
        if not es.indices.exists(index=INDEX):
            es.indices.create(
                index=INDEX,
                settings={"number_of_replicas": 0},
                mappings={
                    "properties": {
                        "pmid": {"type": "keyword"},
                        "question": {"type": "text"},
                        "context": {"type": "text"},
                        "long_answer": {"type": "text"},
                        "labels": {"type": "keyword"},
                        "final_decision": {"type": "keyword"},
                    }
                },
            )

        for record in records:
            es.index(
                index=INDEX,
                id=str(record["pmid"]),
                document=record,
            )

        es.indices.refresh(index=INDEX)
        count = es.count(index=INDEX)["count"]
        print(f"Documents in {INDEX}: {count}")

    print("\nQuestions you can use to test your search:")
    for record in records[:3]:
        print("-", record["question"])


if __name__ == "__main__":
    main()