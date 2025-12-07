import sys
import logging
from mcp.server.fastmcp import FastMCP
from typing import Any
import httpx

# ⚠️ stdio 서버는 stdout에 로그를 찍으면 프로토콜이 깨집니다.
# 반드시 stderr로 로깅하세요. :contentReference[oaicite:3]{index=3}
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

mcp = FastMCP("job-tools", json_response=True)

@mcp.tool()
def normalize_job_posting(
    title: str,
    company: str,
    deadline_iso: str | None = None,
    url: str | None = None,
) -> dict:
    """채용 공고 핵심 필드를 정규화해서 JSON으로 반환"""
    return {
        "title": title.strip(),
        "company": company.strip(),
        "deadline": deadline_iso,
        "url": url,
    }

if __name__ == "__main__":
    # 로컬 붙이기는 보통 stdio가 가장 단순합니다. (stdio는 표준 전송) :contentReference[oaicite:4]{index=4}
    mcp.run(transport="stdio")
