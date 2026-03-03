import pytest

from idr_api.job_manager import SQLiteRunJobManager
from idr_core.secrets import get_secret


def test_get_secret_prefers_file_over_env(tmp_path, monkeypatch):
    secret_path = tmp_path / "secret.txt"
    secret_path.write_text("from-file\n", encoding="utf-8")
    monkeypatch.setenv("IDR_TEST_SECRET", "from-env")
    monkeypatch.setenv("IDR_TEST_SECRET_FILE", str(secret_path))

    assert get_secret("IDR_TEST_SECRET") == "from-file"


def test_get_secret_falls_back_to_env(monkeypatch):
    monkeypatch.delenv("IDR_TEST_SECRET_FILE", raising=False)
    monkeypatch.setenv("IDR_TEST_SECRET", "from-env")

    assert get_secret("IDR_TEST_SECRET") == "from-env"


def test_get_secret_raises_on_unreadable_file(tmp_path, monkeypatch):
    missing_path = tmp_path / "missing-secret.txt"
    monkeypatch.setenv("IDR_TEST_SECRET_FILE", str(missing_path))

    with pytest.raises(RuntimeError):
        get_secret("IDR_TEST_SECRET")


def test_run_job_webhook_uses_rotated_file_secret(tmp_path, monkeypatch):
    token_path = tmp_path / "webhook.token"
    token_path.write_text("token-v1\n", encoding="utf-8")

    monkeypatch.setenv("IDR_RUN_JOB_WEBHOOK_URL", "http://example.test/webhook")
    monkeypatch.setenv("IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN_FILE", str(token_path))
    monkeypatch.delenv("IDR_RUN_JOB_WEBHOOK_BEARER_TOKEN", raising=False)

    captured_headers = []

    class _DummyResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def _fake_urlopen(req, timeout):
        del timeout
        captured_headers.append(req.get_header("Authorization"))
        return _DummyResponse()

    monkeypatch.setattr("idr_api.job_manager.urllib_request.urlopen", _fake_urlopen)

    manager = SQLiteRunJobManager(db_path=str(tmp_path / "run_jobs.sqlite3"))
    manager._post_webhook({"event": "first"})

    token_path.write_text("token-v2\n", encoding="utf-8")
    manager._post_webhook({"event": "second"})

    assert captured_headers == ["Bearer token-v1", "Bearer token-v2"]
