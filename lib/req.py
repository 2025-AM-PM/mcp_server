import time
import requests

API = "https://www.wanted.co.kr/api/chaos/navigation/v1/results"

def fetch_wanted(job_group_id=518, limit=20):
    ts_key = str(int(time.time() * 1000))
    params = {
        # ts_key: "",
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

def extract_name_id_position(payload: dict):
    out = []
    for item in payload.get("data", []):
        out.append({
            "id": item.get("id"),                         # 공고 id
            "name": (item.get("company") or {}).get("name"),# 회사명
            "position": item.get("position"),             # 포지션명
        })
    return out

if __name__ == "__main__":
    payload = fetch_wanted(limit=20)
    rows = extract_name_id_position(payload)

    # 보기 좋게 출력
    for r in rows:
        print(f"{r['id']} | {r['name']} | {r['position']}")
