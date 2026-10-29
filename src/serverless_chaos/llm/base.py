"""Minimal LLM abstraction. Anything OpenAI-compatible works (OpenAI, Ollama /v1, vLLM, LM Studio).
Objective 3 (complexity-aware router) will implement this same protocol and pick local vs cloud per call."""
import os
from typing import Dict, List, Protocol


class LLMClient(Protocol):
    def chat(self, messages: List[Dict[str, str]]) -> str: ...


class OpenAICompatClient:
    def __init__(self, model: str, base_url: str = "https://api.openai.com/v1", api_key: str = "",
                 temperature: float = 0.2, timeout_s: float = 120.0, json_mode: bool = True):
        self.model, self.base_url, self.api_key = model, base_url.rstrip("/"), api_key
        self.temperature, self.timeout_s, self.json_mode = temperature, timeout_s, json_mode

    def chat(self, messages: List[Dict[str, str]]) -> str:
        import httpx
        body = {"model": self.model, "messages": messages, "temperature": self.temperature}
        if self.json_mode:
            body["response_format"] = {"type": "json_object"}
        r = httpx.post(f"{self.base_url}/chat/completions", json=body, timeout=self.timeout_s,
                       headers={"Authorization": f"Bearer {self.api_key}"} if self.api_key else {})
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]


class FakeLLM:
    """Scripted responses for tests; records the prompts it was given."""

    def __init__(self, responses: List[str]):
        self.responses, self.calls = list(responses), []

    def chat(self, messages: List[Dict[str, str]]) -> str:
        self.calls.append(messages)
        return self.responses[min(len(self.calls) - 1, len(self.responses) - 1)]


def llm_from_env() -> OpenAICompatClient:
    """SC_LLM_MODEL (required), SC_LLM_BASE_URL (default OpenAI), SC_LLM_API_KEY.
    Local example: SC_LLM_BASE_URL=http://localhost:11434/v1 SC_LLM_MODEL=llama3.1 (Ollama)."""
    model = os.getenv("SC_LLM_MODEL")
    if not model:
        raise RuntimeError("Set SC_LLM_MODEL (and SC_LLM_BASE_URL / SC_LLM_API_KEY) to use the LLM generator.")
    return OpenAICompatClient(model, os.getenv("SC_LLM_BASE_URL", "https://api.openai.com/v1"), os.getenv("SC_LLM_API_KEY", ""))
