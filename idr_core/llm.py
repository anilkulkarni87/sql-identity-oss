"""
Multi-model LLM client and automatic PII masking for sql-identity-oss.

Supports:
- Local Ollama (default: http://localhost:11434/v1)
- OpenAI & any OpenAI-compatible endpoint (vLLM, LM Studio, Azure, Databricks, Groq)
- Anthropic Claude
- Google Gemini

Enforces automatic PII masking whenever the target endpoint is non-local.
"""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

_EMAIL_RE = re.compile(r"\b([A-Za-z0-9._%+-])([A-Za-z0-9._%+-]*)(@[A-Za-z0-9.-]+\.[A-Za-z]{2,})\b")
_PHONE_RE = re.compile(r"(?<!\d)(?:\+?1[-.\s]?)?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})(?!\d)")
_SSN_RE = re.compile(r"\b\d{3}-\d{2}-(\d{4})\b")

_SENSITIVE_KEY_TOKENS = (
    "email",
    "phone",
    "mobile",
    "ssn",
    "tax_id",
    "address",
    "street",
    "first_name",
    "last_name",
    "full_name",
    "identifier_value",
    "raw_value",
)


def is_local_endpoint(provider: str, base_url: Optional[str] = None) -> bool:
    """Return True if the configured LLM endpoint runs locally (e.g. Ollama on localhost)."""
    prov = (provider or "").strip().lower()
    url = (base_url or "").strip()
    if not url:
        return prov == "ollama"
    parsed = urllib.parse.urlparse(url if "://" in url else f"http://{url}")
    host = (parsed.hostname or "").lower()
    return host in ("localhost", "127.0.0.1", "::1", "0.0.0.0", "host.docker.internal")


def mask_pii_text(text: str) -> str:
    """Redact emails, phone numbers, and SSNs inside free-form text."""
    if not text:
        return text

    def _mask_email_match(m: re.Match[str]) -> str:
        first, _, domain = m.group(1), m.group(2), m.group(3)
        dom_parts = domain.lstrip("@").split(".", 1)
        dom_head = dom_parts[0][0] + "***" if dom_parts[0] else "***"
        dom_tail = f".{dom_parts[1]}" if len(dom_parts) > 1 else ""
        return f"{first}***@{dom_head}{dom_tail}"

    masked = _EMAIL_RE.sub(_mask_email_match, text)
    masked = _SSN_RE.sub(r"***-**-\1", masked)
    masked = _PHONE_RE.sub(r"***-***-\3", masked)
    return masked


def mask_pii_payload(obj: Any, parent_key: str = "") -> Any:
    """Recursively mask PII fields in dicts/lists before sending to a remote LLM."""
    if isinstance(obj, dict):
        return {k: mask_pii_payload(v, parent_key=str(k).lower()) for k, v in obj.items()}
    if isinstance(obj, list):
        return [mask_pii_payload(item, parent_key=parent_key) for item in obj]
    if isinstance(obj, str):
        masked = mask_pii_text(obj)
        if masked == obj and any(tok in parent_key for tok in _SENSITIVE_KEY_TOKENS):
            if len(obj) <= 2:
                return "***"
            return f"{obj[0]}***{obj[-1]}"
        return masked
    return obj


def _ollama_root_url(base_url: Optional[str]) -> str:
    raw = (base_url or "http://localhost:11434").strip().rstrip("/")
    if raw.endswith("/v1"):
        raw = raw[:-3]
    return raw


def list_ollama_models(base_url: Optional[str] = None, timeout: float = 2.5) -> List[str]:
    """Query local Ollama `/api/tags` to list available models."""
    root = _ollama_root_url(base_url)
    req = urllib.request.Request(f"{root}/api/tags", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return [m["name"] for m in data.get("models", []) if isinstance(m, dict) and m.get("name")]
    except Exception:
        return []


@dataclass
class LLMConfig:
    provider: str = "disabled"
    model: str = ""
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    mask_pii: Optional[bool] = None
    timeout: float = 45.0

    @property
    def is_enabled(self) -> bool:
        return self.provider.strip().lower() not in ("", "disabled", "none")

    @property
    def is_local(self) -> bool:
        return is_local_endpoint(self.provider, self.base_url)

    @property
    def effective_mask_pii(self) -> bool:
        if self.mask_pii is not None:
            return bool(self.mask_pii)
        return not self.is_local

    def resolved_base_url(self) -> str:
        prov = self.provider.strip().lower()
        if self.base_url and self.base_url.strip():
            return self.base_url.strip().rstrip("/")
        if prov == "ollama":
            return "http://localhost:11434/v1"
        if prov == "openai":
            return "https://api.openai.com/v1"
        if prov == "anthropic":
            return "https://api.anthropic.com/v1"
        if prov == "gemini":
            return "https://generativelanguage.googleapis.com/v1beta"
        return ""

    def resolved_model(self) -> str:
        if self.model and self.model.strip():
            return self.model.strip()
        prov = self.provider.strip().lower()
        defaults = {
            "ollama": "llama3.2",
            "openai": "gpt-4o-mini",
            "anthropic": "claude-3-5-sonnet-latest",
            "gemini": "gemini-2.0-flash",
        }
        return defaults.get(prov, "llama3.2")

    @classmethod
    def from_env(cls) -> "LLMConfig":
        provider = os.environ.get("IDR_LLM_PROVIDER", "disabled").strip().lower()
        model = os.environ.get("IDR_LLM_MODEL", "").strip()
        base_url = os.environ.get("IDR_LLM_BASE_URL", "").strip() or None
        api_key = os.environ.get("IDR_LLM_API_KEY", "").strip() or None
        raw_mask = os.environ.get("IDR_LLM_MASK_PII", "").strip().lower()
        mask_pii: Optional[bool] = None
        if raw_mask in ("1", "true", "yes"):
            mask_pii = True
        elif raw_mask in ("0", "false", "no"):
            mask_pii = False
        return cls(
            provider=provider,
            model=model,
            base_url=base_url,
            api_key=api_key,
            mask_pii=mask_pii,
        )

    def to_public_dict(self) -> Dict[str, Any]:
        """Return safe config representation without exposing the raw API key."""
        return {
            "provider": self.provider,
            "enabled": self.is_enabled,
            "model": self.resolved_model() if self.is_enabled else "",
            "base_url": self.resolved_base_url() if self.is_enabled else "",
            "has_api_key": bool(self.api_key),
            "is_local": self.is_local,
            "mask_pii": self.effective_mask_pii,
        }


class LLMClient:
    """Thin multi-provider chat client with automatic PII masking."""

    def __init__(self, config: Optional[LLMConfig] = None):
        self.config = config or LLMConfig.from_env()

    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.1,
        json_mode: bool = False,
    ) -> str:
        if not self.config.is_enabled:
            raise RuntimeError(
                "AI assistant is disabled. Set IDR_LLM_PROVIDER (e.g. 'ollama') or configure in UI Settings."
            )

        prov = self.config.provider.strip().lower()
        model = self.config.resolved_model()
        base_url = self.config.resolved_base_url()

        safe_messages = messages
        if self.config.effective_mask_pii:
            safe_messages = [
                {"role": m["role"], "content": mask_pii_text(m["content"])} for m in messages
            ]

        if prov in ("ollama", "openai", "openai_compatible"):
            return self._call_openai_compatible(base_url, model, safe_messages, temperature, json_mode)
        if prov == "anthropic":
            return self._call_anthropic(base_url, model, safe_messages, temperature)
        if prov == "gemini":
            return self._call_gemini(base_url, model, safe_messages, temperature, json_mode)
        raise ValueError(f"Unsupported LLM provider: '{self.config.provider}'")

    def _post_json(self, url: str, body: Dict[str, Any], headers: Dict[str, str]) -> Dict[str, Any]:
        payload = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=self.config.timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise RuntimeError(f"LLM HTTP {exc.code} from {url}: {detail[:300]}") from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise RuntimeError(f"Failed to reach LLM endpoint {url}: {exc}") from exc

    def _call_openai_compatible(
        self,
        base_url: str,
        model: str,
        messages: List[Dict[str, str]],
        temperature: float,
        json_mode: bool,
    ) -> str:
        url = f"{base_url}/chat/completions"
        headers = {"Content-Type": "application/json"}
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"
        body: Dict[str, Any] = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
        }
        if json_mode:
            body["response_format"] = {"type": "json_object"}
        data = self._post_json(url, body, headers)
        choices = data.get("choices") or []
        if not choices:
            raise RuntimeError(f"Empty choices in LLM response: {data}")
        return choices[0].get("message", {}).get("content", "")

    def _call_anthropic(
        self,
        base_url: str,
        model: str,
        messages: List[Dict[str, str]],
        temperature: float,
    ) -> str:
        if not self.config.api_key:
            raise RuntimeError("Anthropic provider requires an API key.")
        system_parts = [m["content"] for m in messages if m["role"] == "system"]
        chat_msgs = [m for m in messages if m["role"] != "system"]
        body: Dict[str, Any] = {
            "model": model,
            "max_tokens": 1500,
            "temperature": temperature,
            "messages": chat_msgs,
        }
        if system_parts:
            body["system"] = "\n".join(system_parts)
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.config.api_key,
            "anthropic-version": "2023-06-01",
        }
        data = self._post_json(f"{base_url}/messages", body, headers)
        content = data.get("content") or []
        return "".join(block.get("text", "") for block in content if isinstance(block, dict))

    def _call_gemini(
        self,
        base_url: str,
        model: str,
        messages: List[Dict[str, str]],
        temperature: float,
        json_mode: bool,
    ) -> str:
        if not self.config.api_key:
            raise RuntimeError("Gemini provider requires an API key.")
        system_parts = [m["content"] for m in messages if m["role"] == "system"]
        contents = [
            {
                "role": "model" if m["role"] == "assistant" else "user",
                "parts": [{"text": m["content"]}],
            }
            for m in messages
            if m["role"] != "system"
        ]
        body: Dict[str, Any] = {
            "contents": contents,
            "generationConfig": {"temperature": temperature},
        }
        if json_mode:
            body["generationConfig"]["responseMimeType"] = "application/json"
        if system_parts:
            body["systemInstruction"] = {"parts": [{"text": "\n".join(system_parts)}]}
        url = f"{base_url}/models/{model}:generateContent?key={urllib.parse.quote(self.config.api_key)}"
        data = self._post_json(url, body, {"Content-Type": "application/json"})
        candidates = data.get("candidates") or []
        if not candidates:
            raise RuntimeError(f"Empty candidates in Gemini response: {data}")
        parts = candidates[0].get("content", {}).get("parts", [])
        return "".join(p.get("text", "") for p in parts if isinstance(p, dict))

