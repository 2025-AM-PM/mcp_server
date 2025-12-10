from __future__ import annotations
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
import os

JOB_INDEX_TEMPLATE = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
    "mappings": {
        # 스키마 강제: 아래 properties에 없는 키 들어오면 에러
        "dynamic": "strict",
        "properties": {
            # string|null
            "회사이름": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            },
            "포지션": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            },
            "회사 위치": {
                "type": "text",
                "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
            },
            "employmentType": {"type": "keyword"},
            "url": {"type": "keyword"},

            # string[]  (ES에서는 동일 필드에 여러 값 넣으면 배열로 저장됨)
            "자격 요건": {"type": "text"},
            "주요업무": {"type": "text"},
            "occupationalCategory": {"type": "keyword"},
            "experienceRequirements": {"type": "text"},

            # date|string|null (YYYY-MM-DD만 들어온다는 전제)
            "datePosted": {"type": "date", "format": "yyyy-MM-dd"},
            "validThrough": {"type": "date", "format": "yyyy-MM-dd"},
        }
    },
}

def ensure_job_index(es: Elasticsearch, index_name: str = "jobs") -> None:
    """jobs 인덱스가 없으면, 위 스키마로 생성."""
    if es.indices.exists(index=index_name):
        return
    es.indices.create(index=index_name, body=JOB_INDEX_TEMPLATE)

load_dotenv()
API_KEY = os.getenv("ELASTIC_API")
URL = os.getenv("URL")

es = Elasticsearch(
    URL,
    api_key=API_KEY,
)