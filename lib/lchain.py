from langchain_openai import ChatOpenAI

llm = ChatOpenAI(
    base_url="http://localhost:3434/v1",
    api_key="EMPTY",  # vLLM 서버에서 강제하지 않으면 더미로 OK
    model="Qwen/Qwen2.5-7B-Instruct",
    temperature=0.3,
    top_p=0.8,
    max_tokens=1024,
    # OpenAI 표준 밖 파라미터는 vLLM/OpenAI-client 관례대로 extra_body로 전달
    extra_body={"repetition_penalty": 1.05},
    model_kwargs={"response_format": {"type": "json_object"}},  # ✅ JSON 강제
)
