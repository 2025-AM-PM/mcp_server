from pathlib import Path
from datetime import datetime
from typing import Dict, Any
import json

REQUIRED_KEYS = [
    "url",
]

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