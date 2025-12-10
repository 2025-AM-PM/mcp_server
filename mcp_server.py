import sys
import logging
from mcp.server.fastmcp import FastMCP
from typing import Any, Optional, Dict
import httpx
from lib.req import *

# ⚠️ stdio 서버는 stdout에 로그를 찍으면 프로토콜이 깨집니다.
# 반드시 stderr로 로깅하세요. :contentReference[oaicite:3]{index=3}
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

mcp = FastMCP("job-tools", json_response=True)

@mcp.tool()
def wanted_detail_payload(
    job_data: Dict[str, Any]) -> str:
    try:
        # 여기서 job_data는 원티드/잡코리아/사람인 등 어디서 온 dict든 상관없음
        text = build_llm_payload(job_data)
        return text
    except Exception as e:
        return f"Error: {e}"
        

if __name__ == "__main__":
    # 로컬 붙이기는 보통 stdio가 가장 단순합니다. (stdio는 표준 전송) :contentReference[oaicite:4]{index=4}
    mcp.run(transport="stdio")
