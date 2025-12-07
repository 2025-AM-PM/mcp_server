import sys
import logging
from mcp.server.fastmcp import FastMCP
from typing import Any
import httpx
from lib.req import *

# ⚠️ stdio 서버는 stdout에 로그를 찍으면 프로토콜이 깨집니다.
# 반드시 stderr로 로깅하세요. :contentReference[oaicite:3]{index=3}
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

mcp = FastMCP("job-tools", json_response=True)

@mcp.tool()
def wanted_detail_payload(
    job_id: int,
    company_name: Optional[str] = None,
    position: Optional[str] = None,
) -> str:
    """
    https://www.wanted.co.kr/wd/{id} 를 조회해서 title/meta_description 추출 후
    LLM에 넘길 string(text)로 반환
    """
    try:
        row = {"id": job_id, "name": company_name, "position": position}
        row = {"id": job_id, "name": company_name, "position": position}
        enriched = fetch_and_extract_job_meta(job_id)
        enriched["name"] = company_name
        enriched["position"] = position
        
        text = build_llm_payload(enriched)
        structed_data = {"ok": True, "id": job_id, "text": text, "data": enriched}
        return text
    except Exception as e:
        structed_data = {"ok": False, "id": job_id, "error": str(e), "text": f"id: {job_id}\nerror: {e}"}
        return f"Error: {e}"

if __name__ == "__main__":
    # 로컬 붙이기는 보통 stdio가 가장 단순합니다. (stdio는 표준 전송) :contentReference[oaicite:4]{index=4}
    mcp.run(transport="stdio")
