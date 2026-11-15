"""
FastAPI router for optional multi-model AI assistant (Ollama-first + BYOK).

API keys are stored only in memory/process state (never persisted to idr_meta).
PII masking is automatically enabled for non-local endpoints.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from idr_api.dependencies import get_adapter
from idr_core.ai_assistant import (
    ask_identity_graph,
    explain_cluster_with_ai,
    suggest_config_with_ai,
    test_llm_connection,
    triage_with_ai,
)
from idr_core.llm import LLMClient, LLMConfig, list_ollama_models
from idr_core.profiler import IDRDataProfiler

router = APIRouter(prefix="/api/ai", tags=["ai"])

# Process-local runtime config (initialized from env vars)
_RUNTIME_LLM_CONFIG: LLMConfig = LLMConfig.from_env()


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
    prompt: str
    tables: list[str] = []


@router.get("/config")
def get_ai_config():
    return _RUNTIME_LLM_CONFIG.to_public_dict()


@router.post("/config")
def update_ai_config(payload: AIConfigUpdate):
    global _RUNTIME_LLM_CONFIG
    existing_key = _RUNTIME_LLM_CONFIG.api_key
    new_key = payload.api_key if payload.api_key not in (None, "") else existing_key
    if payload.provider.strip().lower() in ("disabled", "none", ""):
        new_key = None
    _RUNTIME_LLM_CONFIG = LLMConfig(
        provider=payload.provider.strip().lower(),
        model=payload.model.strip(),
        base_url=payload.base_url.strip() if payload.base_url else None,
        api_key=new_key,
        mask_pii=payload.mask_pii,
    )
    return _RUNTIME_LLM_CONFIG.to_public_dict()


@router.get("/ollama/models")
def get_ollama_models(base_url: Optional[str] = None):
    models = list_ollama_models(base_url or _RUNTIME_LLM_CONFIG.base_url)
    return {"models": models}


@router.post("/test")
def test_ai_connection():
    try:
        return test_llm_connection(LLMClient(_RUNTIME_LLM_CONFIG))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/explain")
def explain_cluster(payload: AIExplainRequest, adapter=Depends(get_adapter)):
    try:
        return explain_cluster_with_ai(
            adapter,
            payload.resolved_id,
            client=LLMClient(_RUNTIME_LLM_CONFIG),
            custom_prompt=payload.prompt,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/triage")
def run_ai_triage(payload: AITriageRequest, adapter=Depends(get_adapter)):
    try:
        return triage_with_ai(
            adapter,
            client=LLMClient(_RUNTIME_LLM_CONFIG),
            max_confidence=payload.max_confidence,
            limit=payload.limit,
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/ask")
def ask_ai(payload: AIAskRequest, adapter=Depends(get_adapter)):
    try:
        return ask_identity_graph(
            adapter,
            payload.question,
            client=LLMClient(_RUNTIME_LLM_CONFIG),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/suggest-config")
def suggest_config(payload: AISuggestConfigRequest, adapter=Depends(get_adapter)):
    try:
        tables = payload.tables
        if not tables and hasattr(adapter, "list_tables"):
            tables = adapter.list_tables()[:10]
        profile = IDRDataProfiler(adapter).profile_and_generate_config(tables) if tables else {}
        return suggest_config_with_ai(
            profile,
            payload.prompt,
            client=LLMClient(_RUNTIME_LLM_CONFIG),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

