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
from typing import Any, Dict, Iterable, List, Optional, Tuple

Tuple_Provider_Model = Tuple[str, str]

_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
_EMAIL_RE = re.compile(r"\b([A-Za-z0-9._%+-])([A-Za-z0-9._%+-]*)(@[A-Za-z0-9.-]+\.[A-Za-z]{2,})\b")
_PHONE_RE = re.compile(
    r"(?<![A-Za-z0-9])(?:\+?1[-.\s]?)?\(?(\d{3})\)?[-.\s]?(\d{3})[-.\s]?(\d{4})(?![A-Za-z0-9])"
)
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
    "dob",
    "date_of_birth",
    "birth_date",
    "ip_address",
    "zip_code",
    "postal_code",
)


def is_local_endpoint(provider: str, base_url: Optional[str] = None) -> bool:
    """Return True if the configured LLM endpoint runs locally or inside the warehouse perimeter."""
    prov = (provider or "").strip().lower()
    if prov == "warehouse":
        return True
    url = (base_url or "").strip()
    if not url:
        return prov == "ollama"
    parsed = urllib.parse.urlparse(url if "://" in url else f"http://{url}")
    host = (parsed.hostname or "").lower()
    return host in ("localhost", "127.0.0.1", "::1", "0.0.0.0", "host.docker.internal")


_KNOWN_PROVIDER_PREFIXES = {
    "warehouse",
    "ollama",
    "openai",
    "anthropic",
    "gemini",
    "openai_compatible",
}


def infer_provider_from_model(
    model_name: Optional[str], explicit_provider: Optional[str] = None
) -> Tuple_Provider_Model:
    """Resolve (provider, clean_model) from an optional provider and model string (supports 'provider:model')."""
    raw_model = (model_name or "").strip()
    prov = (explicit_provider or "").strip().lower()
    if ":" in raw_model and not prov:
        maybe_prov, rest = raw_model.split(":", 1)
        if maybe_prov.strip().lower() in _KNOWN_PROVIDER_PREFIXES and rest.strip():
            return maybe_prov.strip().lower(), rest.strip()
    if prov:
        return prov, raw_model
    lower = raw_model.lower()
    if lower.startswith("claude-"):
        return "anthropic", raw_model
    if lower.startswith(("gpt-", "o1", "o3", "o4", "chatgpt-")):
        return "openai", raw_model
    if lower.startswith("gemini-"):
        return "gemini", raw_model
    return "", raw_model


def api_key_for_provider(provider: str) -> Optional[str]:
    """Look up the environment API key matching a specific LLM provider."""
    prov = (provider or "").strip().lower()
    env_prov = os.environ.get("IDR_LLM_PROVIDER", "").strip().lower()
    generic_key = os.environ.get("IDR_LLM_API_KEY", "").strip() or None
    if prov == "openai":
        return os.environ.get("OPENAI_API_KEY", "").strip() or (
            generic_key if env_prov in ("", "openai") else None
        )
    if prov == "anthropic":
        return os.environ.get("ANTHROPIC_API_KEY", "").strip() or (
            generic_key if env_prov in ("", "anthropic") else None
        )
    if prov == "gemini":
        return (
            os.environ.get("GEMINI_API_KEY", "").strip()
            or os.environ.get("GOOGLE_API_KEY", "").strip()
            or (generic_key if env_prov in ("", "gemini") else None)
        )
    if prov == "openai_compatible":
        return generic_key or os.environ.get("OPENAI_API_KEY", "").strip() or None
    return generic_key if env_prov == prov else None


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

    uuids: List[str] = []

    def _stash_uuid(m: re.Match[str]) -> str:
        idx = len(uuids)
        uuids.append(m.group(0))
        return f"__IDR_UUID_{idx}__"

    masked = _UUID_RE.sub(_stash_uuid, text)
    masked = _EMAIL_RE.sub(_mask_email_match, masked)
    masked = _SSN_RE.sub(r"***-**-\1", masked)
    masked = _PHONE_RE.sub(r"***-***-\3", masked)
    for idx, tok in enumerate(uuids):
        masked = masked.replace(f"__IDR_UUID_{idx}__", tok)
    return masked


def mask_pii_payload(
    obj: Any,
    parent_key: str = "",
    extra_sensitive_keys: Optional[Iterable[str]] = None,
) -> Any:
    """Recursively mask PII fields in dicts/lists before sending to a remote LLM."""
    extra_set = (
        {str(k).strip().lower() for k in extra_sensitive_keys if str(k).strip()}
        if extra_sensitive_keys
        else set()
    )

    def _walk(val: Any, key: str) -> Any:
        if isinstance(val, dict):
            return {k: _walk(v, str(k).lower()) for k, v in val.items()}
        if isinstance(val, list):
            return [_walk(item, key) for item in val]
        if isinstance(val, str):
            masked = mask_pii_text(val)
            is_sensitive = any(tok in key for tok in _SENSITIVE_KEY_TOKENS) or (
                key in extra_set
            )
            if masked == val and is_sensitive:
                if len(val) <= 2:
                    return "***"
                return f"{val[0]}***{val[-1]}"
            return masked
        return val

    return _walk(obj, parent_key.lower())


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
    timeout: float = 90.0

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

    def resolved_model(self, dialect: Optional[str] = None) -> str:
        if self.model and self.model.strip():
            return self.model.strip()
        prov = self.provider.strip().lower()
        if prov == "warehouse":
            d = (dialect or "duckdb").strip().lower()
            wh_defaults = {
                "snowflake": "llama3.1-8b",
                "databricks": "databricks-meta-llama-3-1-8b-instruct",
                "bigquery": "",
                "duckdb": "llama3.2",
            }
            return wh_defaults.get(d, "llama3.2")
        defaults = {
            "ollama": "llama3.2",
            "openai": "gpt-4o-mini",
            "anthropic": "claude-3-5-sonnet-latest",
            "gemini": "gemini-2.0-flash",
        }
        return defaults.get(prov, "llama3.2")

    @classmethod
    def from_env(cls) -> "LLMConfig":
        raw_provider = os.environ.get("IDR_LLM_PROVIDER", "").strip().lower()
        model = os.environ.get("IDR_LLM_MODEL", "").strip()
        base_url = os.environ.get("IDR_LLM_BASE_URL", "").strip() or None
        api_key = os.environ.get("IDR_LLM_API_KEY", "").strip() or None

        openai_key = os.environ.get("OPENAI_API_KEY", "").strip() or None
        anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "").strip() or None
        gemini_key = (
            os.environ.get("GEMINI_API_KEY", "").strip()
            or os.environ.get("GOOGLE_API_KEY", "").strip()
            or None
        )

        if raw_provider:
            provider = raw_provider
        elif openai_key:
            provider = "openai"
        elif anthropic_key:
            provider = "anthropic"
        elif gemini_key:
            provider = "gemini"
        else:
            provider = "disabled"

        if not api_key:
            if provider == "openai":
                api_key = openai_key
            elif provider == "anthropic":
                api_key = anthropic_key
            elif provider == "gemini":
                api_key = gemini_key

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
    """Thin multi-provider chat client with automatic PII masking and warehouse-native AI."""

    def __init__(self, config: Optional[LLMConfig] = None, adapter: Any = None):
        self.config = config or LLMConfig.from_env()
        self.adapter = adapter

    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.1,
        json_mode: bool = False,
    ) -> str:
        if not self.config.is_enabled:
            raise RuntimeError(
                "AI assistant is disabled. Set IDR_LLM_PROVIDER (e.g. 'ollama', 'warehouse', or 'openai') or configure in UI Settings."
            )

        prov = self.config.provider.strip().lower()
        dialect = getattr(self.adapter, "dialect", "duckdb") if self.adapter is not None else "duckdb"
        model = self.config.resolved_model(dialect=dialect)
        base_url = self.config.resolved_base_url()

        safe_messages = messages
        if self.config.effective_mask_pii:
            safe_messages = [
                {"role": m["role"], "content": mask_pii_text(m["content"])} for m in messages
            ]

        if prov == "warehouse":
            return self._call_warehouse(model, safe_messages, temperature, json_mode)
        if prov in ("ollama", "openai", "openai_compatible"):
            return self._call_openai_compatible(base_url, model, safe_messages, temperature, json_mode)
        if prov == "anthropic":
            return self._call_anthropic(base_url, model, safe_messages, temperature)
        if prov == "gemini":
            return self._call_gemini(base_url, model, safe_messages, temperature, json_mode)
        raise ValueError(f"Unsupported LLM provider: '{self.config.provider}'")

    def _call_warehouse(
        self,
        model: str,
        messages: List[Dict[str, str]],
        temperature: float,
        json_mode: bool,
    ) -> str:
        """Invoke warehouse-native SQL AI functions (Snowflake Cortex, Databricks ai_query, BigQuery ML, or DuckDB -> local Ollama)."""
        if self.adapter is None:
            raise RuntimeError(
                "Warehouse-native AI provider requires an active warehouse connection."
            )
        dialect = str(getattr(self.adapter, "dialect", "duckdb") or "duckdb").strip().lower()
        if dialect == "duckdb":
            ollama_url = self.config.base_url or "http://localhost:11434/v1"
            return self._call_openai_compatible(
                ollama_url.rstrip("/"), model or "llama3.2", messages, temperature, json_mode
            )

        prompt_parts = []
        for msg in messages:
            role = str(msg.get("role", "user")).upper()
            content = str(msg.get("content", ""))
            prompt_parts.append(f"{role}:\n{content}")
        if json_mode:
            prompt_parts.append("SYSTEM:\nReturn valid JSON only with no markdown fences.")
        full_prompt = "\n\n".join(prompt_parts)
        escaped_prompt = full_prompt.replace("'", "''")
        escaped_model = (model or "").strip().replace("'", "''")

        if dialect == "snowflake":
            sql = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{escaped_model}', '{escaped_prompt}') AS response"
        elif dialect == "databricks":
            sql = f"SELECT ai_query('{escaped_model}', '{escaped_prompt}') AS response"
        elif dialect == "bigquery":
            if not escaped_model:
                raise RuntimeError(
                    "BigQuery warehouse-native AI requires a remote model FQN in 'model' "
                    "(e.g. `my_project.my_dataset.gemini_flash`)."
                )
            safe_bq_model = escaped_model.strip("`")
            sql = (
                f"SELECT ml_generate_text_llm_result AS response "
                f"FROM ML.GENERATE_TEXT(MODEL `{safe_bq_model}`, "
                f"(SELECT '{escaped_prompt}' AS prompt), "
                f"STRUCT({float(temperature)} AS temperature, TRUE AS flatten_json_output))"
            )
        else:
            raise RuntimeError(f"Warehouse-native AI is not supported for dialect '{dialect}'.")

        rows = self.adapter.query(sql)
        if not rows:
            return ""
        first = rows[0]
        val = first.get("response") if isinstance(first, dict) else first[0]
        return str(val or "").strip()

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

    @staticmethod
    def _extract_json_object(raw: str) -> Dict[str, Any]:
        """Extract a JSON object from raw text, handling markdown fences or wrapper prose."""
        text = (raw or "").strip()
        if not text:
            raise ValueError("LLM returned empty response")
        # Direct parse
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass
        # Strip ```json ... ``` fences
        fence_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL | re.IGNORECASE)
        if fence_match:
            parsed = json.loads(fence_match.group(1))
            if isinstance(parsed, dict):
                return parsed
        # Balanced brace extraction from first '{' to last '}'
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end > start:
            parsed = json.loads(text[start : end + 1])
            if isinstance(parsed, dict):
                return parsed
        raise ValueError(f"Could not parse JSON object from LLM response: {text[:300]}")

    def chat_structured(
        self,
        messages: List[Dict[str, str]],
        json_schema: Dict[str, Any],
        schema_name: str = "structured_output",
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> Dict[str, Any]:
        """
        Invoke the configured LLM with provider-native structured JSON Schema enforcement.

        - Ollama: native `/api/chat` with `format: <json_schema>` (grammar-constrained decoding)
        - OpenAI / vLLM / OpenAI-compatible: `response_format: {type: json_schema, ...}` with
          graceful fallback to `json_object`
        - Anthropic: forced `tool_use` with `input_schema: <json_schema>`
        - Gemini: `responseMimeType: application/json`
        """
        if not self.config.is_enabled:
            raise RuntimeError(
                "AI assistant is disabled. Set IDR_LLM_PROVIDER (e.g. 'ollama') or configure in UI Settings."
            )

        prov = self.config.provider.strip().lower()
        dialect = getattr(self.adapter, "dialect", "duckdb") if self.adapter is not None else "duckdb"
        model = self.config.resolved_model(dialect=dialect)
        base_url = self.config.resolved_base_url()

        safe_messages = messages
        if self.config.effective_mask_pii:
            safe_messages = [
                {"role": m["role"], "content": mask_pii_text(m["content"])} for m in messages
            ]

        if prov == "warehouse" and str(dialect).strip().lower() != "duckdb":
            schema_instruction = (
                "Return a single valid JSON object strictly conforming to this JSON Schema "
                "(no markdown prose or explanations):\n" + json.dumps(json_schema)
            )
            wh_messages = list(safe_messages) + [{"role": "system", "content": schema_instruction}]
            raw = self._call_warehouse(model, wh_messages, temperature, json_mode=True)
            return self._extract_json_object(raw)

        if prov == "ollama" or (prov == "warehouse" and str(dialect).strip().lower() == "duckdb"):
            num_ctx = int(os.environ.get("IDR_OLLAMA_NUM_CTX", "16384") or "16384")
            ollama_base = base_url or "http://localhost:11434/v1"
            root = _ollama_root_url(ollama_base)
            body = {
                "model": model or "llama3.2",
                "messages": safe_messages,
                "stream": False,
                "format": json_schema,
                "options": {
                    "temperature": temperature,
                    "num_predict": min(int(max_tokens), 3072),
                    "num_ctx": num_ctx,
                },
            }
            try:
                data = self._post_json(f"{root}/api/chat", body, {"Content-Type": "application/json"})
                raw = (data.get("message") or {}).get("content", "")
                return self._extract_json_object(raw)
            except RuntimeError as exc:
                # Only fall back to OpenAI-compatible endpoint if server rejects /api/chat schema (not on timeouts)
                err_str = str(exc)
                if any(code in err_str for code in ("HTTP 400", "HTTP 404", "HTTP 405", "HTTP 422")):
                    raw = self._call_openai_compatible(
                        ollama_base, model or "llama3.2", safe_messages, temperature, json_mode=True
                    )
                    return self._extract_json_object(raw)
                raise

        if prov in ("openai", "openai_compatible"):
            url = f"{base_url}/chat/completions"
            headers = {"Content-Type": "application/json"}
            if self.config.api_key:
                headers["Authorization"] = f"Bearer {self.config.api_key}"
            body = {
                "model": model,
                "messages": safe_messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "response_format": {
                    "type": "json_schema",
                    "json_schema": {
                        "name": schema_name,
                        "schema": json_schema,
                        "strict": False,
                    },
                },
            }
            try:
                data = self._post_json(url, body, headers)
                choices = data.get("choices") or []
                raw = choices[0].get("message", {}).get("content", "") if choices else ""
                return self._extract_json_object(raw)
            except RuntimeError as exc:
                # Some OpenAI-compatible servers only support json_object
                if "HTTP 400" in str(exc) or "HTTP 422" in str(exc):
                    raw = self._call_openai_compatible(base_url, model, safe_messages, temperature, json_mode=True)
                    return self._extract_json_object(raw)
                raise

        if prov == "anthropic":
            if not self.config.api_key:
                raise RuntimeError("Anthropic provider requires an API key.")
            system_parts = [m["content"] for m in safe_messages if m["role"] == "system"]
            chat_msgs = [m for m in safe_messages if m["role"] != "system"]
            body = {
                "model": model,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "messages": chat_msgs,
                "tools": [
                    {
                        "name": schema_name,
                        "description": "Return the structured JSON output matching the schema.",
                        "input_schema": json_schema,
                    }
                ],
                "tool_choice": {"type": "tool", "name": schema_name},
            }
            if system_parts:
                body["system"] = "\n".join(system_parts)
            headers = {
                "Content-Type": "application/json",
                "x-api-key": self.config.api_key,
                "anthropic-version": "2023-06-01",
            }
            data = self._post_json(f"{base_url}/messages", body, headers)
            for block in data.get("content") or []:
                if isinstance(block, dict) and block.get("type") == "tool_use" and isinstance(block.get("input"), dict):
                    return block["input"]
            raw = "".join(
                block.get("text", "") for block in (data.get("content") or []) if isinstance(block, dict)
            )
            return self._extract_json_object(raw)

        if prov == "gemini":
            raw = self._call_gemini(base_url, model, safe_messages, temperature, json_mode=True)
            return self._extract_json_object(raw)

        raise ValueError(f"Unsupported LLM provider: '{self.config.provider}'")

