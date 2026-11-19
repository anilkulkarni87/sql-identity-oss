"""
FastAPI router for optional multi-model AI assistant (Ollama-first + BYOK).

API keys are stored only in memory/process state (never persisted to idr_meta).
PII masking is automatically enabled for non-local endpoints.
"""

from __future__ import annotations

from threading import RLock
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from idr_api.dependencies import get_adapter, get_current_user, get_user_key
from idr_core.ai_assistant import (
    ask_identity_graph,
    copilot_chat,
    explain_cluster_with_ai,
    suggest_config_with_ai,
    test_llm_connection,
    triage_with_ai,
)
from idr_core.llm import LLMClient, LLMConfig, list_ollama_models
from idr_core.profiler import IDRDataProfiler

router = APIRouter(prefix="/api/ai", tags=["ai"])


class LLMConfigStore:
    """Thread-safe, user-scoped runtime LLM config store (avoids global singleton state)."""

    def __init__(self) -> None:
        self._configs: Dict[str, LLMConfig] = {}
        self._lock = RLock()

    def get(self, user_key: str) -> LLMConfig:
        with self._lock:
            return self._configs.get(user_key) or LLMConfig.from_env()

    def set(self, user_key: str, config: LLMConfig) -> None:
        with self._lock:
            self._configs[user_key] = config


_DEFAULT_LLM_STORE = LLMConfigStore()


def get_llm_config_store() -> LLMConfigStore:
    return _DEFAULT_LLM_STORE


class AIConfigUpdate(BaseModel):
    provider: str = "ollama"
    model: str = ""
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    mask_pii: Optional[bool] = None


class AIExplainRequest(BaseModel):
    resolved_id: str
    prompt: Optional[str] = None


class AITriageRequest(BaseModel):
    max_confidence: float = 0.65
    limit: int = 8


class AIAskRequest(BaseModel):
    question: str


class AISuggestConfigRequest(BaseModel):
    prompt: str = "Suggest entity keys, identifier mappings, matching rules, survivorship rules, and multi-lens views."
    goal: Optional[str] = None
    tables: list[str] = []
    sample_size: int = 2000
    verify_sql: bool = True


@router.get("/config")
def get_ai_config(
    current_user: dict = Depends(get_current_user),
    store: LLMConfigStore = Depends(get_llm_config_store),
):
    return store.get(get_user_key(current_user)).to_public_dict()


@router.post("/config")
def update_ai_config(
    payload: AIConfigUpdate,
    current_user: dict = Depends(get_current_user),
    store: LLMConfigStore = Depends(get_llm_config_store),
):
    user_key = get_user_key(current_user)
    existing = store.get(user_key)
    new_key = payload.api_key if payload.api_key not in (None, "") else existing.api_key
    if payload.provider.strip().lower() in ("disabled", "none", ""):
        new_key = None
    updated = LLMConfig(
        provider=payload.provider.strip().lower(),
        model=payload.model.strip(),
        base_url=payload.base_url.strip() if payload.base_url else None,
        api_key=new_key,
        mask_pii=payload.mask_pii,
    )
    store.set(user_key, updated)
    return updated.to_public_dict()


@router.get("/ollama/models")
def get_ollama_models(
    base_url: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    store: LLMConfigStore = Depends(get_llm_config_store),
):
    cfg = store.get(get_user_key(current_user))
    models = list_ollama_models(base_url or cfg.base_url)
    return {"models": models}


@router.post("/test")
def test_ai_connection(
    current_user: dict = Depends(get_current_user),
    store: LLMConfigStore = Depends(get_llm_config_store),
):
    try:
        cfg = store.get(get_user_key(current_user))
        return test_llm_connection(LLMClient(cfg))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/explain")
def explain_cluster(
    payload: AIExplainRequest,
    adapter=Depends(get_adapter),
    current_user: dict = Depends(get_current_user),
    store: LLMConfigStore = Depends(get_llm_config_store),
):
    try:
        cfg = store.get(get_user_key(current_user))
        return explain_cluster_with_ai(
            adapter,
            payload.resolved_id,
            client=LLMClient(cfg),
            custom_prompt=payload.prompt,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/triage")
def run_ai_triage(
    payload: AITriageRequest,
    adapter=Depends(get_adapter),
    current_user: dict = Depends(get_current_user),
    store: LLMConfigStore = Depends(get_llm_config_store),
):
    try:
        cfg = store.get(get_user_key(current_user))
        return triage_with_ai(
            adapter,
            client=LLMClient(cfg),
            max_confidence=payload.max_confidence,
            limit=payload.limit,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/ask")
def ask_ai(
    payload: AIAskRequest,
    adapter=Depends(get_adapter),
    current_user: dict = Depends(get_current_user),
    store: LLMConfigStore = Depends(get_llm_config_store),
):
    try:
        cfg = store.get(get_user_key(current_user))
        return ask_identity_graph(
            adapter,
            payload.question,
            client=LLMClient(cfg),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class AICopilotRequest(BaseModel):
    message: str
    history: Optional[List[Dict[str, str]]] = None


@router.post("/copilot")
def copilot(
    payload: AICopilotRequest,
    adapter=Depends(get_adapter),
    current_user: dict = Depends(get_current_user),
    store: LLMConfigStore = Depends(get_llm_config_store),
):
    try:
        cfg = store.get(get_user_key(current_user))
        return copilot_chat(
            adapter,
            payload.message,
            history=payload.history,
            client=LLMClient(cfg),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/suggest-config")
def suggest_config(
    payload: AISuggestConfigRequest,
    adapter=Depends(get_adapter),
    current_user: dict = Depends(get_current_user),
    store: LLMConfigStore = Depends(get_llm_config_store),
):
    try:
        from idr_core.profiler import is_system_table

        tables = [t for t in (payload.tables or []) if t and not is_system_table(t)]
        if not tables and hasattr(adapter, "list_tables"):
            tables = [t for t in adapter.list_tables() if not is_system_table(t)][:15]
        cfg = store.get(get_user_key(current_user))
        goal_text = (payload.goal or payload.prompt or "").strip()
        if tables:
            return suggest_config_with_ai(
                {},
                goal_text,
                client=LLMClient(cfg),
                adapter=adapter,
                tables=tables,
                sample_size=payload.sample_size,
                verify_sql=payload.verify_sql,
            )
        profile = IDRDataProfiler(adapter).profile_and_generate_config(tables) if tables else {}
        return suggest_config_with_ai(
            profile,
            goal_text,
            client=LLMClient(cfg),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

