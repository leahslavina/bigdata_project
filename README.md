# PubMed Big Data Search and AI Question-Answering Pipeline

This project implements an end-to-end data pipeline for 1,000 semi-structured PubMed records.

The records are streamed through Apache Kafka, processed by a Python consumer, stored in Elasticsearch, and made searchable through a FastAPI application. The project also includes a retrieval-augmented generation (RAG) endpoint that uses Gemini to answer questions based on retrieved PubMed records.

## Architecture

```text
pubmed_data.json
        |
        v
   producer.py
        |
        v
 Apache Kafka
(pubmed-topic)
        |
        v
   consumer.py
        |
        v
 Elasticsearch
(pubmed-index)
        |
        v
 search_engine.py
        |
        v
 FastAPI
 /search and /ask
        |
        v
 Gemini RAG answer
```

## Technologies

- Python
- Docker and Docker Compose
- Apache Kafka
- Apache Zookeeper
- Elasticsearch
- FastAPI
- Uvicorn
- Google Gemini API

## Data

The project uses `pubmed_data.json`, containing 1,000 PubMed question-and-answer records.

Each record may contain:

- `pmid`
- `question`
- `context`
- `long_answer`
- `final_decision`
- `labels`

The dataset contains semi-structured JSON and unstructured medical text.

## Pipeline

1. `producer.py` reads the PubMed records and sends them to the Kafka topic `pubmed-topic`.
2. `consumer.py` reads the messages from Kafka.
3. The consumer transforms the messages into Elasticsearch documents and indexes them in `pubmed-index`.
4. `search_engine.py` searches the indexed text.
5. `app.py` exposes the search system through a FastAPI API.
6. `answer_generator.py` sends retrieved evidence to Gemini and generates a grounded answer with PMID citations.

## Requirements

Before running the project, install:

- Docker Desktop
- Python 3.12 or a compatible Python 3 version
- Git

A Gemini API key is required only for the `/ask` AI endpoint. The `/search` endpoint works without it.

## Installation

Clone the repository and enter the project directory:

```bash
git clone https://github.com/leahslavina/bigdata_project.git
cd bigdata_project
git switch solo/complete-pipeline
```

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install the Python packages:

```bash
python -m pip install -r requirements.txt
```

## Running the project

Start Docker Desktop first.

Start Kafka, Zookeeper, and Elasticsearch:

```bash
docker compose up -d
```

Check that the services are running:

```bash
docker compose ps
```

### Terminal 1: Start the consumer

```bash
cd ~/bigdata_project
source .venv/bin/activate
python consumer.py
```

Leave this terminal running.

### Terminal 2: Run the producer

```bash
cd ~/bigdata_project
source .venv/bin/activate
python producer.py
```

The producer sends the 1,000 PubMed records and then finishes.

### Verify Elasticsearch

```bash
curl http://localhost:9200/pubmed-index/_count
```

The response should show:

```json
{
  "count": 1000
}
```

### Terminal 3: Start the API

Set the Gemini API key without saving it in the source code:

```bash
cd ~/bigdata_project
source .venv/bin/activate
read -s "GEMINI_API_KEY?Paste your Gemini API key: "
export GEMINI_API_KEY
echo
python -m uvicorn app:app --reload --port 8002
```

Open the interactive API documentation:

http://localhost:8002/docs

## API endpoints

### `GET /`

Returns basic information about the API.

### `POST /search`

Runs keyword-based Elasticsearch retrieval.

Example request:

```json
{
  "query": "strabismus amblyopia",
  "top_k": 3
}
```

### `POST /ask`

Retrieves relevant PubMed documents and asks Gemini to generate an answer based only on the retrieved evidence.

Example request:

```json
{
  "query": "Do mitochondria play a role in programmed cell death in lace plant leaves?",
  "top_k": 3
}
```

The response contains:

- The user’s question
- A generated answer
- PMID source citations
- The retrieved Elasticsearch documents

If the retrieved documents do not contain enough information, the AI returns:

```text
The retrieved articles do not provide enough evidence.
```

## AI capability

The project implements retrieval-augmented generation (RAG):

1. The user submits a natural-language question.
2. Elasticsearch retrieves relevant PubMed records.
3. The retrieved records are supplied to Gemini as evidence.
4. Gemini generates a grounded answer.
5. The response includes PMID citations to the supporting records.
6. The prompt prevents the model from presenting unsupported information as evidence.

The Gemini API key is read from the `GEMINI_API_KEY` environment variable and must never be committed to GitHub.

## Stopping the project

Stop the consumer and Uvicorn using `Control + C` in their terminals.

Stop the Docker services:

```bash
docker compose down
```

This preserves the Elasticsearch volume.

To delete the stored Docker data and perform a completely clean run, use:

```bash
docker compose down -v
```

Warning: the `-v` option permanently deletes the project’s Docker volume and indexed data.