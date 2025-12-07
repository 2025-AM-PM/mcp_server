# ✅ 디버깅 옵션/로그 추가 버전
import time
import re
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

API = "https://www.wanted.co.kr/api/chaos/navigation/v1/results"
DETAIL_URL = "https://www.wanted.co.kr/wd/{id}"

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

def fetch_and_extract_job_meta(job_id: int, *, session: requests.Session | None = None) -> dict:
    html = fetch_job_html(job_id, session=session)
    meta = extract_title_and_description(html, job_id=job_id)
    meta["id"] = job_id
    meta["url"] = DETAIL_URL.format(id=job_id)
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

def build_llm_payload(job_row: dict, meta: dict) -> str:
    parts = [
        f"id: {job_row.get('id')}",
        f"company_name: {job_row.get('name')}",
        f"position: {job_row.get('position')}",
        f"title_tag: {meta.get('title')}",
        f"meta_description: {meta.get('description')}",
        f"url: {meta.get('url')}",
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
                    out.append({**j, "title_tag": meta["title"], "meta_description": meta["description"], "url": meta["url"]})
                except Exception as e:
                    out.append({**j, "title_tag": None, "meta_description": None, "url": DETAIL_URL.format(id=j["id"]), "error": str(e)})
    return out

if __name__ == "__main__":
    payload = fetch_wanted(limit=3)  # 디버그할 땐 3개만
    rows = extract_name_id_position(payload)

    enriched = enrich_jobs_with_detail_meta(rows, max_workers=3)

    # 1개만 LLM payload로 출력
    meta = {"title": enriched[0]["title_tag"], "description": enriched[0]["meta_description"], "url": enriched[0]["url"]}
    llm_payload = build_llm_payload(enriched[0], meta)
    print("\n=== LLM PAYLOAD ===")
    print(llm_payload)
