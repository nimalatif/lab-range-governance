import json
import os
import urllib.request
import urllib.error
from typing import Any, Dict, Optional


# We only allow these intents (keeps it deterministic + safe)
_ALLOWED_INTENTS = {"list_today", "explain_result", "help"}

# JSON Schema for Structured Outputs (strict)
_INTENT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "intent": {"type": "string", "enum": ["list_today", "explain_result", "help"]},
        "limit": {"type": "integer", "minimum": 1, "maximum": 200},
        "result_id": {
            "type": "string",
            # Accept your known patterns: R1002, obs-glu-002, etc.
            "pattern": r"^(R\d{3,}|obs-[A-Za-z0-9-]+)$",
        },
    },
    "required": ["intent"],
}

_SYSTEM_INSTRUCTIONS = (
    "You are a routing function. Return ONLY valid JSON matching the provided schema. "
    "Do not include any prose. Choose the best intent for the user's message."
)


def _bool_env(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if v in {"1", "true", "yes", "y", "on"}:
        return True
    if v in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _extract_output_text(resp_json: Dict[str, Any]) -> str:
    """
    OpenAI Responses API returns output as an array of items.
    We extract the first assistant message output_text.
    """
    out = resp_json.get("output")
    if not isinstance(out, list):
        return ""

    for item in out:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "message":
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for c in content:
            if isinstance(c, dict) and c.get("type") == "output_text":
                t = c.get("text")
                return t if isinstance(t, str) else ""
    return ""


def route_intent(user_text: str, *, default_limit: int = 25, timeout_s: int = 10) -> Optional[Dict[str, Any]]:
    """
    Returns a dict like:
      {"intent":"list_today","limit":25}
      {"intent":"explain_result","result_id":"R1002"}
    or None (so caller can fall back to deterministic rules).
    """
    if not _bool_env("AGENT_LLM_ENABLED", default=False):
        return None

    provider = (os.getenv("LLM_PROVIDER", "openai") or "").strip().lower()
    if provider != "openai":
        # Keep simple for now: only OpenAI. Caller will fall back.
        return None

    api_key = (os.getenv("OPENAI_API_KEY", "") or "").strip()
    model = (os.getenv("OPENAI_MODEL", "gpt-4o-mini") or "").strip()
    if not api_key or not model:
        return None

    base_url = (os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1") or "").strip().rstrip("/")
    url = f"{base_url}/responses"

    # Keep payload small + deterministic
    user_text = (user_text or "").strip()
    if not user_text:
        return None
    if len(user_text) > 2000:
        user_text = user_text[:2000]

    req_body: Dict[str, Any] = {
        "model": model,
        "temperature": 0,
        "max_output_tokens": 80,
        "input": [
            {
                "role": "system",
                "content": [{"type": "input_text", "text": _SYSTEM_INSTRUCTIONS}],
            },
            {"role": "user", "content": [{"type": "input_text", "text": user_text}]},
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "agent_intent",
                "schema": _INTENT_SCHEMA,
                "strict": True,
            }
        },
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    try:
        data = json.dumps(req_body).encode("utf-8")
        request = urllib.request.Request(url, data=data, headers=headers, method="POST")
        with urllib.request.urlopen(request, timeout=timeout_s) as resp:
            raw = resp.read()
        resp_json = json.loads(raw.decode("utf-8"))

        text = _extract_output_text(resp_json).strip()
        if not text:
            return None

        obj = json.loads(text)
        if not isinstance(obj, dict):
            return None

        intent = obj.get("intent")
        if intent not in _ALLOWED_INTENTS:
            return None

        # Normalize / sanitize
        if intent == "list_today":
            lim = obj.get("limit", default_limit)
            if not isinstance(lim, int):
                lim = default_limit
            lim = max(1, min(lim, 200))
            return {"intent": "list_today", "limit": lim}

        if intent == "explain_result":
            rid = obj.get("result_id")
            if not isinstance(rid, str) or not rid.strip():
                return None
            return {"intent": "explain_result", "result_id": rid.strip()}

        return {"intent": "help"}

    except Exception:
        # Any failure -> fall back to deterministic routing
        return None
