import asyncio, hashlib, json
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain.agents import create_agent
from pathlib import Path
from datetime import datetime
from elasticsearch import helpers

from lib.lchain import llm
from lib.req import *
from lib.util import *
from db.server import es


def make_doc_id(doc: dict) -> str:
    base = doc.get("url") or json.dumps(doc, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

async def main():
    index_name = "jobs"
    # ndjson_path = Path("./out") / f"jobs_{datetime.now().strftime('%Y%m%d')}.ndjson"

    client = MultiServerMCPClient(
        {
            "wanted": {
                "transport": "stdio",
                "command": "python",
                "args": ["mcp_server.py"],
            },
        }
    )

    SYSTEM_PROMPT = """역할: 채용공고 구조화(Structuring) 에이전트

목표
- 입력으로 받은 “공고 문자열”(예: id/company_name/position/title_tag/meta_description/url 포함)을 분석해
- 아래 스키마의 JSON 객체 1개를 “오직 JSON만” 반환한다.

입력 형식(예시)
첫번째 버전:
  - id: ...
  - company_name: ...
  - position: ...
  - title_tag: ...
  - meta_description: ...
  - url: ...
두번째 버전:
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
반드시 두 버전 중 하나를 사용 할 것.

출력 스키마(반드시 이 키만 사용)
{
  "회사이름": string|null,
  "포지션": string|null,
  "회사 위치": string|null,
  "자격 요건": string[],            // 없으면 []
  "주요업무": string[],             // 없으면 []
  "employmentType": string|null,     // 정규직/계약직/인턴 등 명시된 경우만, 없으면 null
  "datePosted": string|null,         // ISO-8601(YYYY-MM-DD)로 명시된 경우만, 없으면 null
  "occupationalCategory": string[],  // 입력에서 확실한 경우만, 없으면 []
  "validThrough": string|null,       // 마감일/지원마감이 명시된 경우만 ISO-8601, 없으면 null
  "experienceRequirements": string[],// “n년 이상/신입/경력” 등 경험 요구사항만 추려서, 없으면 []
  "url": string|null
}

규칙(중요)
1) 절대 추측하지 말 것
- 입력 문자열에 근거가 없는 값은 만들지 않는다.
- 모호하면 null 또는 [].

2) meta_description 파싱 규칙
- meta_description 내에서 다음 라벨을 탐색해 섹션을 분리:
  - "회사 위치:" / "회사위치:" / "근무지:" / "위치:"
  - "자격 요건:" / "자격요건:" / "Requirements:"
  - "주요 업무:" / "주요업무:" / "업무:" / "Responsibilities:"
  - "마감:" / "마감일:" / "지원 마감:" / "validThrough:" 등
- 불릿은 아래 형태를 모두 허용:
  - "• 항목", "- 항목", "· 항목", "1) 항목", 줄바꿈
- 불릿을 배열로 만들 때는:
  - 앞뒤 공백 제거, 비어있으면 제외
  - 같은 항목 중복 제거

3) experienceRequirements 구성
- "자격 요건" 항목 중 경험/연차/신입/경력 관련 문장만 추려 배열로 만든다.
  예: "Python 기반 ... 1년 이상", "경력 무관", "신입 가능"
- 경험 요구가 meta_description 다른 구간에 분명히 있으면(예: "경력: 3년+") 그것도 포함.

4) 날짜 형식
- validThrough/datePosted는 입력에 “정확한 날짜”가 있을 때만 ISO-8601(YYYY-MM-DD)로 변환.
- “상시채용/채용시 마감/마감 임박”처럼 날짜가 없으면 null.

5) 출력 형식 강제
- 반환은 JSON 한 덩어리만.
- 코드블록, 설명, 주석, 추가 텍스트 금지.

검증 체크리스트(반환 직전 자체 점검)
- 모든 키가 스키마와 정확히 일치하는가?
- 배열 필드는 항상 배열인가?
- 근거 없는 값(추측)이 들어가 있지 않은가?
- JSON 파싱 가능한가?
"""
    tools = await client.get_tools()
    tool_map = {t.name: t for t in tools}

    payload = fetch_wanted(limit=20)
    # 원티드 크롤링
    rows = extract_name_id_position(payload)
    enriched = enrich_jobs_with_detail_meta(rows, max_workers=3)

    # 잡코리아 크롤링
    job_item = extract_jobkorea_metadata()
    items = enriched + job_item

    # 사람인 크롤링
    saram_item = fetch_saramin_list_html()
    content = parse_saramin_list_html(saram_item)
    items = items + content
    actions = []
    for row in items:
        # 2) 상세 1건 문자열 확보 (LLM에 한 번에 하나만)
        text = await tool_map["wanted_detail_payload"].ainvoke({"job_data": row})
        
        print(text)
        # 3) 구조화 LLM 호출(1건 -> JSON 1개)
        msg = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": text},
        ]
        out = await llm.ainvoke(msg)

        # (선택) JSON 파싱 확인
        obj = json.loads(out.content)
        obj_normalize = normalize_record(row, obj)
        doc = coerce_job_record(obj_normalize)
        # append_ndjson(ndjson_path, obj_normalize)            # 누적 NDJSON

        actions.append({
            "_op_type": "index",
            "_index": index_name,
            "_id": make_doc_id(doc),
            "_source": doc,
        })

        if actions:
          helpers.bulk(es, actions)
          es.indices.refresh(index=index_name)


asyncio.run(main())
