
import time
import re
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import re
from typing import Any, Dict, Optional, List
import html as htmlmod
from urllib.parse import urljoin
from datetime import date
import copy

"""
최종 스키마:
  "회사이름": string|null,
  "포지션": string|null,
  "회사 위치": string|null,
  "자격 요건": string[],            // 없으면 []
  "주요업무": string[],             // 없으면 []
  "employmentType": string[]|null,     // 정규직/계약직/인턴 등 명시된 경우만, 없으면 null
  "datePosted": string|null,         // ISO-8601(YYYY-MM-DD)로 명시된 경우만, 없으면 null
  "occupationalCategory": string[],  // 입력에서 확실한 경우만, 없으면 []
  "validThrough": string|null,       // 마감일/지원마감이 명시된 경우만 ISO-8601, 없으면 null
  "experienceRequirements": string[],// “n년 이상/신입/경력” 등 경험 요구사항만 추려서, 없으면 []
  "url": string|null
"""

API = "https://www.wanted.co.kr/api/chaos/navigation/v1/results"
DETAIL_URL = "https://www.wanted.co.kr/wd/{id}"
_TRAILING_COMMA_RE = re.compile(r",\s*([}\]])")  # , } / , ] 제거
LIST_URL = "https://www.jobkorea.co.kr/Top100/?Main_Career_Type=1&Search_Type=1&BizJobtype_Bctgr_Code=10031&BizJobtype_Bctgr_Name=AI%C2%B7%EA%B0%9C%EB%B0%9C%C2%B7%EB%8D%B0%EC%9D%B4%ED%84%B0&BizJobtype_Code=0&BizJobtype_Name=AI%C2%B7%EA%B0%9C%EB%B0%9C%C2%B7%EB%8D%B0%EC%9D%B4%ED%84%B0+%EC%A0%84%EC%B2%B4&Major_Big_Code=0&Major_Big_Name=%EC%A0%84%EC%B2%B4&Major_Code=0&Major_Name=%EC%A0%84%EC%B2%B4&Edu_Level_Code=9&Edu_Level_Name=%EC%A0%84%EC%B2%B4&Edu_Level_Name=%EC%A0%84%EC%B2%B4&MidScroll=&duty-depth1=on"
JOBURL = "https://www.jobkorea.co.kr"

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

FIELD_ORDER = [
    "회사이름",
    "포지션",
    "title",
    "title_tag",
    "description",
    "meta_description",
    "url",
    "employmentType",
    "datePosted",
    "occupationalCategory",
    "validThrough",
    "experienceRequirements",
]

DATA_STRUCT = {
        "회사이름": None,
        "포지션": None,
        "회사 위치": None,
        "자격 요건": [],
        "주요업무": [],
        "employmentType": None,
        "datePosted": None,
        "occupationalCategory": [],
        "validThrough": None,
        "experienceRequirements": [],
        "url": None,
}

DEBUG = True  # 필요 시 False

def _parse_iso_date(text: str) -> Optional[str]:
    """
    YYYY.MM.DD / YYYY-MM-DD / YYYY/MM/DD 형태만 ISO-8601(YYYY-MM-DD)로 변환.
    그 외(상시채용, 채용시 마감, D-3 등)는 None.
    """
    if not text:
        return None

    # 명시적인 날짜가 아닌 표현은 스킵
    if any(kw in text for kw in ["상시", "채용시", "마감", "오늘", "D-"]):
        return None

    m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", text)
    if not m:
        return None

    y, mth, d = m.groups()
    return f"{int(y):04d}-{int(mth):02d}-{int(d):02d}"

def fetch_saramin_list_html():
    url = "https://www.saramin.co.kr/zf_user/jobs/list/job-category?cat_mcls=2&panel_type=&search_optional_item=n&search_done=y&panel_count=y&preview=y"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.saramin.co.kr/zf_user/jobs/list/job-category",
        "Upgrade-Insecure-Requests": "1",
        # requests 기본 압축 처리가 안정적이라서 br/zstd는 굳이 명시하지 않는 게 안전
        "Accept-Encoding": "gzip, deflate",
    }
    response = requests.get(url, headers=headers, timeout=20)
    response.raise_for_status()
    return response.text

def parse_saramin_list_html(
    html: str,
    base_url: str = "https://www.saramin.co.kr",
) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    ul = soup.select_one("ul.list_product.list_grand") or soup
    items = ul.select("li.item.lookup")

    results: list[dict] = []
    for it in items:
        # 리스트/중첩 구조까지 안전하게 복사 (공고 간 리스트 공유 방지)
        data: dict = copy.deepcopy(DATA_STRUCT)

        a = it.select_one("a[href]")
        if a:
            data["url"] = urljoin(base_url, a.get("href", "").strip()) or None

        tit = it.select_one("strong.tit")
        if tit:
            data["포지션"] = tit.get_text(strip=True) or None

        corp = it.select_one("span.corp")
        if corp:
            data["회사이름"] = corp.get_text(strip=True) or None

        loc = it.select_one("li.company_local")
        if loc:
            data["회사 위치"] = loc.get_text(strip=True) or None

        # 마감일: <span class="date">~12.24(수)</span>
        deadline = it.select_one("span.date")
        if deadline:
            raw = deadline.get_text(strip=True)
            data["validThrough"] = _parse_iso_date(raw) or raw  # today 쓰려면 함수가 받도록

        # 경험(신입/경력 등)
        exp_li = None
        loc_li = it.select_one("li.company_local")
        if loc_li:
            exp_li = loc_li.find_next("li")
        if exp_li:
            exp_txt = exp_li.get_text(" ", strip=True)
            if exp_txt:
                data["experienceRequirements"] = [exp_txt]

        # 학력(고졸 이상 등)
        edu_li = exp_li.find_next("li") if exp_li else None
        if edu_li:
            edu_txt = edu_li.get_text(" ", strip=True).replace("이상", " 이상").strip()
            if edu_txt:
                data["자격 요건"].append(edu_txt)

        # 경험도 자격요건에 포함(원치 않으면 삭제)
        if data.get("experienceRequirements"):
            data["자격 요건"].insert(0, data["experienceRequirements"][0])

        results.append(data)

    return results

def parse_jobkorea_li(li) -> Dict[str, Any]:
    """
    JobKorea Top100 목록의 <li> 한 개를 받아서
    지정한 스키마 dict를 반환.
    """
    # 기본 스켈레톤
    data = DATA_STRUCT.copy()

    # ---- id (옵션) ----
    src_raw = li.get("data-source")
    if src_raw:
        try:
            src = json.loads(src_raw)
            job_id = src.get("gno") or src.get("giNo")
            if job_id is not None:
                data["id"] = str(job_id)
        except json.JSONDecodeError:
            pass

    # ---- 회사이름 ----
    co_link = li.select_one("a.coLink")
    if co_link:
        data["회사이름"] = co_link.get_text(strip=True) or None

    # ---- 포지션 (공고 제목) ----
    title_link = li.select_one("a.link span")
    if title_link:
        data["포지션"] = title_link.get_text(strip=True) or None

    # ---- occupationalCategory (직무 카테고리) ----
    s_tit = li.select_one("div.sTit")
    if s_tit:
        cats = [span.get_text(strip=True) for span in s_tit.select("span")]
        data["occupationalCategory"] = [c for c in cats if c]

    # ---- sDsc(경력/학력/지역/고용형태) 파싱 ----
    s_dsc = li.select_one("div.sDsc")
    desc_spans = []
    if s_dsc:
        desc_spans = [span.get_text(strip=True) for span in s_dsc.select("span")]

    experience_terms = []
    employment_type = None
    location = None

    for t in desc_spans:
        # 경험 요건
        if any(k in t for k in ["신입", "경력", "년", "이상", "이하", "무관"]):
            experience_terms.append(t)

        # 고용형태
        if employment_type is None and any(k in t for k in ["정규직", "계약직", "인턴", "알바", "파트", "수습"]):
            employment_type = t

    # 위치 후보: 경력/경험·고용형태에 해당되지 않는 span 중 첫 번째
    location_candidates = [
        t for t in desc_spans
        if t not in experience_terms and t != employment_type
    ]
    if location_candidates:
        location = location_candidates[0]

    data["회사 위치"] = location or None
    data["employmentType"] = employment_type or None
    data["experienceRequirements"] = experience_terms

    # ---- 마감일(validThrough) ----
    day_span = li.select_one("div.side span.day")
    if day_span:
        raw_day = day_span.get_text(strip=True)
        data["validThrough"] = _parse_iso_date(raw_day)

    # ---- url ----
    detail_link = li.select_one("a.link")
    if detail_link and detail_link.has_attr("href"):
        href = detail_link["href"]
        if href.startswith("/"):
            data["url"] = JOBURL + href
        else:
            data["url"] = href

    # 자격 요건 / 주요업무는 리스트 페이지에 없으므로 [] 유지
    # datePosted도 명시된 날짜가 보이지 않으면 None 유지

    return data


def _dbg(msg: str):
    if DEBUG:
        print(f"[DEBUG] {msg}")


def extract_jobkorea_metadata() -> list[dict]:
    resp = requests.get(LIST_URL, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    items = soup.select("ol.rankList > li")
    results = [parse_jobkorea_li(li) for li in items]
    return results


def fetch_wanted(job_group_id=518, limit=20):
    params = {
        "job_group_id": str(job_group_id),
        "country": "kr",
        "job_sort": "job.recommend_order",
        "years": "-1",
        "locations": "all",
        "limit": str(limit),
        "job_sort": "job.latest_order"
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

def build_llm_payload(data: Dict[str, Any]) -> str:
    parts: List[str] = []

    # 1) 우선 중요 필드들만 지정된 순서대로 찍고
    for key in FIELD_ORDER:
        if key in data:
            parts.append(f"{key}: {data[key]}")

    # 2) 나머지 키들도 필요하면 추가로 찍고 싶다면 (옵션)
    #    이미 찍은 키는 제외
    printed = set(FIELD_ORDER)
    for key, value in data.items():
        if key not in printed:
            parts.append(f"{key}: {value}")

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
    results = fetch_saramin_list_html()
    content = job_item = parse_saramin_list_html(results)
    text= build_llm_payload(content[0])
    print(content)