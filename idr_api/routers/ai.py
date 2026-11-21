"""
FastAPI router for optional multi-model AI assistant (Ollama-first + BYOK).

API keys are stored only in memory/process state (never persisted to idr_meta).
PII masking is automatically enabled for non-local endpoints.
"""

from __future__ import annotations

from threading import RLock
from typing import Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from idr_api.dependencies import get_adapter, get_current_user, get_user_key, require_write_access
from idr_core.ai_assistant import (
    ask_identity_graph,
    copilot_chat,
    explain_cluster_with_ai,
    suggest_config_with_ai,
    test_llm_connection,
    triage_with_ai,
)
from idr_core.ai_config import AIConfigContext, draft_hash
from idr_core.ai_review import AIReviewService, StaleReview
from idr_core.llm import LLMClient, LLMConfig, list_ollama_models

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
    tables: list[str] = Field(default_factory=list, max_length=50)
    sample_size: int = Field(default=2000, ge=1, le=10000)
    mode: Literal["config", "rules"] = "config"
    current_config: Optional[dict] = None
    context: Optional[AIConfigContext] = None
    base_draft_hash: Optional[str] = None
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


def _generation_options(payload):
    context = payload.context
    if context is None and payload.current_config:
        raw = payload.current_config.get("ai_assistance")
        context = AIConfigContext.model_validate(raw) if raw else None
    if context is None:
        raise HTTPException(422, "Select a structured AI configuration policy first.")
    if payload.mode == "rules" and not payload.current_config:
        raise HTTPException(422, "Rules generation requires the current draft.")
    if payload.base_draft_hash and payload.base_draft_hash != draft_hash(
        payload.current_config or {}
    ):
        raise HTTPException(409, "Draft changed; regenerate the proposal.")
    return dict(
        context=context,
        current_config=payload.current_config,
        mode=payload.mode,
        base_draft_hash=payload.base_draft_hash,
    )


@router.post("/suggest-config")
def suggest_config(
    payload: AISuggestConfigRequest,
    adapter=Depends(get_adapter),
    current_user: dict = Depends(get_current_user),
    store: LLMConfigStore = Depends(get_llm_config_store),
):
    try:
        from idr_core.profiler import is_system_table

        options = _generation_options(payload)

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
                **options,
            )
        raise HTTPException(422, "No readable source tables selected.")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/suggest-config/stream")
def suggest_config_stream(
    payload: AISuggestConfigRequest,
    adapter=Depends(get_adapter),
    current_user: dict = Depends(get_current_user),
    store: LLMConfigStore = Depends(get_llm_config_store),
):
    """Stream live stage-by-stage progress events (SSE) followed by the final suggested config."""
    import json
    import queue
    import threading

    from fastapi.responses import StreamingResponse

    from idr_core.profiler import is_system_table

    options = _generation_options(payload)
    tables = [t for t in (payload.tables or []) if t and not is_system_table(t)]
    if not tables and hasattr(adapter, "list_tables"):
        tables = [t for t in adapter.list_tables() if not is_system_table(t)][:15]
    cfg = store.get(get_user_key(current_user))
    goal_text = (payload.goal or payload.prompt or "").strip()

    ev_queue: queue.Queue = queue.Queue()

    def _on_progress(evt: dict) -> None:
        ev_queue.put(("progress", evt))

    def _worker() -> None:
        try:
            if tables:
                res = suggest_config_with_ai(
                    {},
                    goal_text,
                    client=LLMClient(cfg),
                    adapter=adapter,
                    tables=tables,
                    sample_size=payload.sample_size,
                    verify_sql=payload.verify_sql,
                    on_progress=_on_progress,
                    **options,
                )
            else:
                raise ValueError("No readable source tables selected")
            ev_queue.put(("result", res))
        except Exception as exc:
            ev_queue.put(("error", {"detail": str(exc)}))
        finally:
            ev_queue.put(("done", None))

    threading.Thread(target=_worker, daemon=True).start()

    def _event_generator():
        while True:
            try:
                kind, data = ev_queue.get(timeout=15.0)
            except queue.Empty:
                yield ": keepalive\n\n"
                continue
            if kind == "done":
                break
            yield f"event: {kind}\ndata: {json.dumps(data, default=str)}\n\n"

    return StreamingResponse(
        _event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


class AIDryRunCritiqueRequest(BaseModel):
    config: dict = Field(default_factory=dict)
    dry_run_summary: Optional[dict] = None
    warnings: List[str] = Field(default_factory=list)
    run_id: str
    draft_hash: str
    lens: str = "default"


@router.post("/critique-dry-run")
def critique_dry_run(
    payload: AIDryRunCritiqueRequest,
    adapter=Depends(get_adapter),
    current_user: dict = Depends(get_current_user),
    store: LLMConfigStore = Depends(get_llm_config_store),
):
    """Analyze zero-PII Dry Run graph telemetry and return actionable one-click refinement patches."""
    try:
        cfg = store.get(get_user_key(current_user))
        if payload.draft_hash != draft_hash(payload.config):
            raise StaleReview("Draft changed; run a fresh preview")
        return AIReviewService(adapter, get_user_key(current_user)).explain(
            payload.run_id, payload.draft_hash, payload.lens, LLMClient(cfg), payload.config
        )
    except StaleReview as exc:
        raise HTTPException(409, str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


class AICompareRequest(BaseModel):
    run_id: str
    config: dict
    patch: dict
    lens: str = "default"


class AIPatchRequest(BaseModel):
    config: dict
    patch: dict


@router.post("/compare")
def compare_config(
    payload: AICompareRequest,
    adapter=Depends(get_adapter),
    current_user: dict = Depends(require_write_access),
):
    try:
        return AIReviewService(adapter, get_user_key(current_user)).compare(
            payload.run_id, payload.config, payload.patch, payload.lens
        )
    except StaleReview as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@router.post("/compare/stream")
def compare_config_stream(
    payload: AICompareRequest,
    adapter=Depends(get_adapter),
    current_user: dict = Depends(require_write_access),
):
    import json
    import queue
    import threading

    from fastapi.responses import StreamingResponse

    service = AIReviewService(adapter, get_user_key(current_user))
    try:
        service.get(payload.run_id, draft_hash(payload.config), payload.lens)
        service.patch(payload.config, payload.patch)
    except StaleReview as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc
    events = queue.Queue()

    def worker():
        try:
            result = service.compare(
                payload.run_id,
                payload.config,
                payload.patch,
                payload.lens,
                progress=lambda event: events.put(("progress", event)),
            )
            events.put(("result", result))
        except Exception as exc:
            events.put(("error", {"detail": str(exc)}))
        finally:
            events.put(("done", None))

    threading.Thread(target=worker, daemon=True).start()

    def stream():
        while True:
            try:
                kind, data = events.get(timeout=15)
            except queue.Empty:
                yield ": keepalive\n\n"
                continue
            if kind == "done":
                break
            yield f"event: {kind}\ndata: {json.dumps(data, default=str)}\n\n"

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/patch")
def apply_draft_patch(
    payload: AIPatchRequest,
    adapter=Depends(get_adapter),
    current_user: dict = Depends(get_current_user),
):
    try:
        candidate = AIReviewService(adapter, get_user_key(current_user)).patch(
            payload.config, payload.patch
        )
        return {"config": candidate, "draft_hash": draft_hash(candidate), "review_stale": True}
    except StaleReview as exc:
        raise HTTPException(409, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(422, str(exc)) from exc


@router.get("/reviews/{run_id}")
def get_review(
    run_id: str, adapter=Depends(get_adapter), current_user: dict = Depends(get_current_user)
):
    try:
        return AIReviewService(adapter, get_user_key(current_user)).get(run_id)
    except StaleReview as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/reviews/purge")
def purge_reviews(adapter=Depends(get_adapter), current_user: dict = Depends(require_write_access)):
    return {
        "purged": AIReviewService(adapter, get_user_key(current_user)).purge(
            all_owned=True, limit=100
        )
    }
