# ✅ 디버깅 옵션/로그 추가 버전
import time
import re
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import re
from typing import Any, Dict, Optional, Tuple
import html as htmlmod

API = "https://www.wanted.co.kr/api/chaos/navigation/v1/results"
DETAIL_URL = "https://www.wanted.co.kr/wd/{id}"
_TRAILING_COMMA_RE = re.compile(r",\s*([}\]])")  # , } / , ] 제거

DETAIL_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    ),
    "referer": "https://www.wanted.co.kr/",
}

DEBUG = True  # 필요 시 False

def _dbg(msg: str):
    if DEBUG:
        print(f"[DEBUG] {msg}")

def fetch_wanted(job_group_id=518, limit=20):
    params = {
        "job_group_id": str(job_group_id),
        "country": "kr",
        "job_sort": "job.recommend_order",
        "years": "-1",
        "locations": "all",
        "limit": str(limit),
    }
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "ko-KR,ko;q=0.9",
        "user-agent": "Mozilla/5.0",
        "wanted-user-agent": "user-web",
        "wanted-user-country": "KR",
        "wanted-user-language": "ko",
        "referer": "https://www.wanted.co.kr/wdlist/518?country=kr&job_sort=job.recommend_order&years=-1&locations=all",
    }
    r = requests.get(API, params=params, headers=headers, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_job_html(job_id: int, *, session: requests.Session | None = None,
                   timeout: int = 20, max_retries: int = 3) -> str:
    url = DETAIL_URL.format(id=job_id)
    s = session or requests.Session()

    last_err = None
    for i in range(max_retries):
        try:
            r = s.get(url, headers=DETAIL_HEADERS, timeout=timeout)
            _dbg(f"GET {url} -> status={r.status_code}, len={len(r.text)}")
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            _dbg(f"retry {i+1}/{max_retries} failed: {e}")
            time.sleep(0.5 * (2 ** i))
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")

def extract_title_and_description(html: str, *, job_id: int | None = None) -> dict:
    """meta 태그 파싱 디버깅 포함"""
    soup = BeautifulSoup(html, "html.parser")

    # 1) title
    title = None
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    # 2) description meta
    desc = None
    desc_tag = soup.find("meta", attrs={"name": "description"})
    if desc_tag and desc_tag.get("content"):
        desc = desc_tag["content"].strip()

    # 3) fallback: og:description
    og_desc = None
    og_tag = soup.find("meta", attrs={"property": "og:description"})
    if og_tag and og_tag.get("content"):
        og_desc = og_tag["content"].strip()

    if not desc and og_desc:
        desc = og_desc

    # ===== 디버깅 출력 =====
    # if DEBUG:
    #     jid = f"{job_id}" if job_id is not None else "?"
    #     _dbg(f"[{jid}] title parsed: {repr(title)[:200]}")
    #     _dbg(f"[{jid}] meta[name=description] found: {desc_tag is not None}")
    #     if desc_tag is not None:
    #         # meta 태그 자체를 보여주면 가장 빠르게 원인 파악 가능
    #         _dbg(f"[{jid}] meta[name=description] tag: {str(desc_tag)[:400]}")
    #     _dbg(f"[{jid}] meta[property=og:description] found: {og_tag is not None}")
    #     if og_tag is not None:
    #         _dbg(f"[{jid}] meta[property=og:description] tag: {str(og_tag)[:400]}")
    #     _dbg(f"[{jid}] final description len: {0 if desc is None else len(desc)}")

    #     # 4) head에 있는 meta들 일부 샘플 출력(원인 파악용)
    #     head = soup.head
    #     if head:
    #         metas = head.find_all("meta")
    #         _dbg(f"[{jid}] head meta count: {len(metas)}")
    #         # name/ property 있는 meta만 20개까지 출력
    #         shown = 0
    #         for m in metas:
    #             if m.get("name") or m.get("property"):
    #                 _dbg(f"[{jid}] head meta: {str(m)[:250]}")
    #                 shown += 1
    #                 if shown >= 20:
    #                     break

    #     # 5) 혹시 HTML이 SPA 뼈대인지 빠르게 확인 (script가 비정상적으로 많은 경우)
    #     scripts = soup.find_all("script")
    #     _dbg(f"[{jid}] script count: {len(scripts)}")
    #     if title is None and desc is None:
    #         _dbg(f"[{jid}] title/desc 둘 다 None -> JS 렌더링/차단 가능성. HTML 앞부분 dump:")
    #         _dbg(html[:800].replace("\n", "\\n"))

    return {"title": title, "description": desc}

def fetch_and_extract_job_meta(
    job_id: int,
    *,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    html = fetch_job_html(job_id, session=session)

    # ✅ extract_title_and_description가 job_id 인자를 안 받으면 여기서 제거
    meta = extract_title_and_description(html)  # 또는 함수 정의를 job_id 받게 수정

    ld = extract_jobposting_jsonld_fields(html)

    # ✅ jsonld 파서 실패/타입 불일치 방어
    if not isinstance(ld, dict):
        ld = {}

    # id/url 기본 필드
    meta["id"] = job_id
    meta["url"] = DETAIL_URL.format(id=job_id)

    # ✅ 충돌 가능하면 우선순위 결정: jsonld가 덮어써도 되는지 판단
    # 예: title/description은 meta가 더 신뢰되면 ld를 뒤에서 merge하지 않기
    meta.update(ld)

    return meta

def extract_name_id_position(payload: dict):
    out = []
    for item in payload.get("data", []):
        out.append({
            "id": item.get("id"),
            "name": (item.get("company") or {}).get("name"),
            "position": item.get("position"),
        })
    return out

def build_llm_payload(enriched: dict) -> str:
    parts = [
        f"id: {enriched.get('id')}",
        f"company_name: {enriched.get('name')}",
        f"position: {enriched.get('position')}",
        f"title_tag: {enriched.get('title') or enriched.get('title_tag')}",
        f"meta_description: {enriched.get('description') or enriched.get('meta_description')}",
        f"url: {enriched.get('url')}",
    ]
    return "\n".join(parts)

def enrich_jobs_with_detail_meta(jobs: list[dict], *, max_workers: int = 6) -> list[dict]:
    out = []
    with requests.Session() as s:
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = {ex.submit(fetch_and_extract_job_meta, j["id"], session=s): j for j in jobs}
            for fut in as_completed(futs):
                j = futs[fut]
                try:
                    meta = fut.result()
                    out.append({
                        **j,
                        "title_tag": meta.get("title"),
                        "meta_description": meta.get("description"),
                        "url": meta.get("url"),
                        "employmentType": meta.get("employmentType"),
                        "datePosted": meta.get("datePosted"),
                        "occupationalCategory": meta.get("occupationalCategory"),
                        "validThrough": meta.get("validThrough"),
                        "experienceRequirements": meta.get("experienceRequirements"),
                    })
                except Exception as e:
                    out.append({**j, "title_tag": None, "meta_description": None, "url": DETAIL_URL.format(id=j["id"]), "error": str(e)})
    return out


def extract_jobposting_jsonld_fields(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})

    candidates = []
    for sc in scripts:
        raw = (sc.string or sc.get_text() or "").strip()
        if not raw:
            continue
        raw = _TRAILING_COMMA_RE.sub(r"\1", raw)
        try:
            obj = json.loads(raw)
        except Exception:
            continue

        if isinstance(obj, list):
            candidates.extend([x for x in obj if isinstance(x, dict)])
        elif isinstance(obj, dict):
            candidates.append(obj)

    def score(o: Dict[str, Any]) -> int:
        s = 0
        t = o.get("@type")
        if t == "JobPosting" or (isinstance(t, list) and "JobPosting" in t):
            s += 10
        for k in ("employmentType", "datePosted", "occupationalCategory", "validThrough", "experienceRequirements"):
            if k in o:
                s += 2
        return s

    best = max(candidates, key=score, default=None)
    if not best:
        return {}

    valid_through = best.get("validThrough")
    if valid_through in (None, "", [], {}):
        valid_through = "상시채용"

    return {
        "employmentType": best.get("employmentType"),
        "datePosted": best.get("datePosted"),
        "occupationalCategory": best.get("occupationalCategory"),
        "validThrough": best.get("validThrough"),
        "experienceRequirements": best.get("experienceRequirements"),
    }

if __name__ == "__main__":
    payload = fetch_wanted(limit=3)
    rows = extract_name_id_position(payload)

    enriched = enrich_jobs_with_detail_meta(rows, max_workers=3)

    first = enriched[0]
    meta = {
        "title": first.get("title_tag"),
        "description": first.get("meta_description"),
        "url": first.get("url"),
        "employmentType": first.get("employmentType"),
        "datePosted": first.get("datePosted"),
        "occupationalCategory": first.get("occupationalCategory"),
        "validThrough": first.get("validThrough"),
        "experienceRequirements": first.get("experienceRequirements"),
    }
    llm_payload = build_llm_payload(first, meta)
    print("\n=== LLM PAYLOAD ===")
    print(llm_payload)
