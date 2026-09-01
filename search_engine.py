import os

from elasticsearch import Elasticsearch

ES_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
INDEX = os.getenv("ELASTICSEARCH_INDEX", "pubmed-dev-index")


def search(query: str, top_k: int = 5):
    query = query.strip()
    if not query:
        raise ValueError("Please enter a question.")
    if not 1 <= top_k <= 20:
        raise ValueError("top_k must be between 1 and 20.")

    with Elasticsearch(ES_URL) as es:
        response = es.search(
            index=INDEX,
            size=top_k,
            query={
                "multi_match": {
                    "query": query,
                    "fields": [
                        "question^3",
                        "context",
                        "long_answer",
                    ],
                }
            },
        )

    results = []
    for hit in response["hits"]["hits"]:
        record = hit["_source"]
        results.append({
            "pmid": record["pmid"],
            "question": record["question"],
            "context": record.get("context", ""),
            "long_answer": record.get("long_answer", ""),
            "final_decision": record.get("final_decision", ""),
            "score": hit["_score"],
        })

    return results


if __name__ == "__main__":
    matches = search("mitochondria lace plant", top_k=3)

    for match in matches:
        print("Question:", match["question"])
        print("Score:", match["score"])
        print()