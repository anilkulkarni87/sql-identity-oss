import json

from idr_core.doctor import DoctorCheck, collect_doctor_report, run_doctor


def test_collect_doctor_report_shape():
    report = collect_doctor_report()
    assert "status" in report
    assert "counts" in report
    assert "checks" in report
    assert isinstance(report["checks"], list)
    assert report["counts"]["pass"] >= 1


def test_run_doctor_json_output(capsys):
    rc = run_doctor(strict=False, json_output=True)
    out = capsys.readouterr().out
    payload = json.loads(out)
    assert "checks" in payload
    assert rc in (0, 1)


def test_run_doctor_strict_escalates_warning(monkeypatch):
    monkeypatch.setenv("IDR_ALLOW_INSECURE_DEV_AUTH", "true")
    rc = run_doctor(strict=True, json_output=False)
    assert rc in (1, 2)


def test_run_doctor_telemetry_hook(monkeypatch):
    captured = {}

    class _DummyResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def _fake_urlopen(req, timeout):
        del timeout
        captured["url"] = req.full_url
        captured["body"] = req.data.decode("utf-8")
        return _DummyResponse()

    monkeypatch.setattr("idr_core.doctor.urllib_request.urlopen", _fake_urlopen)
    rc = run_doctor(strict=False, json_output=True, telemetry_url="https://example.test/doctor")
    assert rc in (0, 1)
    assert captured["url"] == "https://example.test/doctor"
    assert "\"checks\"" in captured["body"]


def test_collect_doctor_report_cluster_missing_urls_fails():
    report = collect_doctor_report(target="cluster")
    checks = {c["name"]: c for c in report["checks"]}
    assert checks["cluster_api_health"]["status"] == "fail"
    assert checks["cluster_metrics"]["status"] == "fail"


def test_collect_doctor_report_cluster_endpoint_success(monkeypatch):
    class _DummyResponse:
        def __init__(self, status, body):
            self.status = status
            self._body = body

        def read(self):
            return self._body.encode("utf-8")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def _fake_urlopen(req, timeout):
        del timeout
        url = req.full_url
        if url.endswith("/api/health"):
            return _DummyResponse(200, '{"status":"healthy"}')
        if url.endswith("/metrics"):
            return _DummyResponse(200, "idr_http_requests_total 1")
        if url.endswith("/api/auth/whoami"):
            return _DummyResponse(200, '{"sub":"demo"}')
        raise AssertionError(f"unexpected URL: {url}")

    def _fake_check_command(name, command, severity, help_text):
        del command, severity, help_text
        return DoctorCheck(name=name, status="pass", severity="required", detail="ok")

    monkeypatch.setenv("IDR_TOKEN", "dummy-token")
    monkeypatch.setattr("idr_core.doctor.urllib_request.urlopen", _fake_urlopen)
    monkeypatch.setattr("idr_core.doctor._check_command", _fake_check_command)

    report = collect_doctor_report(
        target="cluster",
        api_url="https://idr.example.com/api/health",
        metrics_url="https://idr.example.com/metrics",
        whoami_url="https://idr.example.com/api/auth/whoami",
    )

    checks = {c["name"]: c for c in report["checks"]}
    assert checks["cluster_api_health"]["status"] == "pass"
    assert checks["cluster_metrics"]["status"] == "pass"
    assert checks["cluster_whoami_auth"]["status"] == "pass"
