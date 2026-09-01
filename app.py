from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator
from elasticsearch import ApiError
from elastic_transport import ConnectionError, ConnectionTimeout

from search_engine import search as search_articles

app = FastAPI(title="PubMed Search API")


class SearchRequest(BaseModel):
    query: str = Field(min_length=1)
    top_k: int = Field(default=5, ge=1, le=20)

    @field_validator("query")
    @classmethod
    def validate_query(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Please enter a question.")
        return value


@app.get("/")
def root():
    return {"message": "PubMed Search API is running"}


@app.post("/search")
def search(request: SearchRequest):
    try:
        results = search_articles(request.query, request.top_k)
    except (ConnectionError, ConnectionTimeout) as exc:
        raise HTTPException(
            status_code=503,
            detail="Cannot connect to Elasticsearch.",
        ) from exc
    except ApiError as exc:
        raise HTTPException(
            status_code=503,
            detail="Elasticsearch could not complete the search.",
        ) from exc

    return {
        "query": request.query,
        "top_k": request.top_k,
        "results": results,
    }