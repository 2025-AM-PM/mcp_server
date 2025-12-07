from pathlib import Path
from datetime import datetime
from typing import Dict, Any
import json

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