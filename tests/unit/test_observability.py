"""Logging format, the last-resort 500 handler, metrics labels, and the CLI."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import httpx

from helpers import build_config
from relay.__main__ import main
from relay.app import create_app
from relay.logging_config import JsonFormatter, TextFormatter, configure_logging
from relay.metrics import method_label, status_class

ROOT = Path(__file__).resolve().parents[2]


def make_record(**fields) -> logging.LogRecord:
    record = logging.LogRecord("relay.test", logging.INFO, __file__, 1, "request", (), None)
    record.fields = fields
    return record


def test_json_log_lines_carry_structured_fields() -> None:
    line = JsonFormatter().format(make_record(request_id="abc", status=200))
    payload = json.loads(line)
    assert payload["event"] == "request"
    assert payload["request_id"] == "abc"
    assert payload["status"] == 200
    assert "TextFormatter" not in line
    assert "request_id=abc" in TextFormatter().format(make_record(request_id="abc"))


def test_configure_logging_is_idempotent(monkeypatch) -> None:
    monkeypatch.setenv("RELAY_LOG_JSON", "false")
    configure_logging("DEBUG")
    configure_logging("DEBUG")
    logger = logging.getLogger("relay")
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0].formatter, TextFormatter)
    configure_logging("INFO", json_output=True)


async def test_unexpected_exception_returns_json_500_without_trace(caplog) -> None:
    app = create_app(build_config(["http://127.0.0.1:9"]))

    async def explode(request):
        raise RuntimeError("secret internal detail")

    app.state.gateway.handle = explode
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
            response = await client.get("/api/catalog/x")
            metrics = (await client.get("/metrics")).text
    assert response.status_code == 500
    body = response.json()
    assert body["error"]["code"] == "internal_error"
    assert body["error"]["request_id"] == response.headers["x-request-id"]
    assert "secret internal detail" not in response.text
    assert 'relay_requests_total{method="GET",route="_unmatched",status_class="5xx"} 1.0' in metrics


def test_metric_label_normalisation_is_bounded() -> None:
    assert method_label("get") == "GET"
    assert method_label("BREW") == "OTHER"
    assert status_class(204) == "2xx"
    assert status_class(999) == "other"


def test_cli_check_validates_config(capsys, tmp_path) -> None:
    assert main(["--config", str(ROOT / "config" / "routes.yaml"), "--check"]) == 0
    assert "config OK: 5 route(s)" in capsys.readouterr().out
    bad = tmp_path / "bad.yaml"
    bad.write_text("routes: []\n")
    assert main(["--config", str(bad), "--check"]) == 2
    assert "invalid Relay configuration" in capsys.readouterr().err
