from __future__ import annotations
from pathlib import Path
from datetime import datetime
from typing import Dict, Any
import json
from elasticsearch import Elasticsearch

REQUIRED_KEYS = [
    "url",
]

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

SCHEMA_KEYS = {
    "회사이름",
    "포지션",
    "회사 위치",
    "자격 요건",
    "주요업무",
    "employmentType",
    "datePosted",
    "occupationalCategory",
    "validThrough",
    "experienceRequirements",
    "url",
}

ARRAY_KEYS = {"자격 요건", "주요업무", "occupationalCategory", "experienceRequirements"}
NULLABLE_STRING_KEYS = {"회사이름", "포지션", "회사 위치", "employmentType", "datePosted", "validThrough", "url"}

def coerce_job_record(obj: dict[str, Any]) -> dict[str, Any]:
    """
    LLM/정규화 결과를 '사용자 스키마'에 맞게 강제 변환:
    - 키 누락 보정
    - 배열/문자열/null 타입 보정
    - 스키마 밖 키 제거(ES dynamic=strict 대응)
    """
    out: dict[str, Any] = {}

    # 1) 스키마 키만 유지
    for k in SCHEMA_KEYS:
        out[k] = obj.get(k, None)

    # 2) 배열 필드는 항상 list[str]
    for k in ARRAY_KEYS:
        v = out.get(k)
        if v is None:
            out[k] = []
        elif isinstance(v, list):
            out[k] = [str(x).strip() for x in v if str(x).strip()]
        else:
            # 문자열 하나로 오면 1개짜리 리스트로
            s = str(v).strip()
            out[k] = [s] if s else []

    # 3) nullable string 필드는 str 또는 None
    for k in NULLABLE_STRING_KEYS:
        v = out.get(k)
        if v is None:
            continue
        s = str(v).strip()
        out[k] = s if s else None

    return out

def ensure_job_index(es: Elasticsearch, index_name: str = "jobs") -> None:
    """jobs 인덱스가 없으면, 위 스키마로 생성."""
    if es.indices.exists(index=index_name):
        return
    es.indices.create(index=index_name, body=JOB_INDEX_TEMPLATE)


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding=encoding)
    tmp.replace(path)

def save_job_json(out_dir: Path, job_id: int, obj: Dict[str, Any]) -> Path:
    # 파일명 안전하게: id 기반
    p = out_dir / f"{job_id}.json"
    atomic_write_text(p, json.dumps(obj, ensure_ascii=False, indent=2))
    return p

def append_ndjson(ndjson_path: Path, obj: Dict[str, Any]) -> None:
    ndjson_path.parent.mkdir(parents=True, exist_ok=True)
    with ndjson_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def normalize_record(source: dict, llm_json: dict) -> dict:
    result = dict(llm_json)

    # 빠진 키는 일단 기본값으로 채워넣기
    for key in REQUIRED_KEYS:
        if key not in result:
            if key in ["자격 요건", "주요업무", "occupationalCategory", "experienceRequirements"]:
                result[key] = []
            else:
                result[key] = None

    # 우리가 알고 있는 값 덮어쓰기 (url, 회사이름, 포지션 등)
    for key in ["url", "회사이름", "포지션", "회사 위치"]:
        if key in source:
            result[key] = source[key]

    return result