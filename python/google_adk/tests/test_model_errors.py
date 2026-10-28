"""Model activity failures carry Temporal's retry decision.

A Gemini request that can never succeed (400 for a bad argument, 401 or 403
for a bad key, 404 for an unknown model) must fail the model activity as
non-retryable, so the workflow fails instead of re-sending the request under
the default unlimited retry policy. Rate limits, timeouts and server errors
stay retryable. The TypeScript and Java Google ADK plugins and
``temporalio.google_genai`` classify these statuses the same way.

A local HTTP server stands in for the Gemini API, so the real ADK ``Gemini``
model and ``google-genai`` client run end to end without credentials.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import MagicMock

import pytest
from google.adk.models.llm_request import LlmRequest
from google.genai.types import Content, Part

from temporalio.api.failure.v1 import Failure
from temporalio.converter import DataConverter
from temporalio.google_adk._model import (
    StreamingInvokeInput,
    invoke_model,
    invoke_model_streaming,
)
from temporalio.testing import ActivityEnvironment

_STATUS_NAMES = {
    400: "INVALID_ARGUMENT",
    401: "UNAUTHENTICATED",
    403: "PERMISSION_DENIED",
    404: "NOT_FOUND",
    408: "DEADLINE_EXCEEDED",
    429: "RESOURCE_EXHAUSTED",
    500: "INTERNAL",
    503: "UNAVAILABLE",
}


class _FakeGemini:
    """Answers every request with one HTTP status and records each request."""

    def __init__(self) -> None:
        self.status = 400
        self.requests = 0
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                length = int(self.headers.get("content-length") or 0)
                if length:
                    self.rfile.read(length)
                fake.requests += 1
                body = json.dumps(
                    {
                        "error": {
                            "code": fake.status,
                            "message": "rejected by the fake Gemini API",
                            "status": _STATUS_NAMES[fake.status],
                        }
                    }
                ).encode()
                self.send_response(fake.status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: object) -> None:
                pass

        self._server = HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self._server.server_port}"
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()


@pytest.fixture
def fake_gemini(monkeypatch: pytest.MonkeyPatch) -> Iterator[_FakeGemini]:
    fake = _FakeGemini()
    monkeypatch.setenv("GOOGLE_API_KEY", "fake-key")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_GENAI_USE_VERTEXAI", raising=False)
    monkeypatch.setenv("GOOGLE_GEMINI_BASE_URL", fake.url)
    yield fake
    fake.close()


def _request() -> LlmRequest:
    return LlmRequest(
        model="gemini-2.5-flash",
        contents=[Content(role="user", parts=[Part(text="hi")])],
    )


async def _run(streaming: bool) -> BaseException:
    if streaming:
        env = ActivityEnvironment(client=MagicMock())
        call = env.run(
            invoke_model_streaming,
            StreamingInvokeInput(
                llm_request=_request(),
                streaming_topic="events",
                streaming_batch_interval=timedelta(milliseconds=100),
            ),
        )
    else:
        call = ActivityEnvironment().run(invoke_model, _request())
    with pytest.raises(BaseException) as raised:
        await call
    return raised.value


def _recorded(exc: BaseException) -> Failure:
    """The failure the worker records for an activity that raised ``exc``."""
    failure = Failure()
    converter = DataConverter.default
    converter.failure_converter.to_failure(exc, converter.payload_converter, failure)
    return failure


@pytest.mark.parametrize("streaming", [False, True], ids=["invoke", "streaming"])
@pytest.mark.parametrize("status", [400, 401, 403, 404])
async def test_permanent_status_is_non_retryable(
    fake_gemini: _FakeGemini, status: int, streaming: bool
) -> None:
    fake_gemini.status = status
    failure = _recorded(await _run(streaming))
    assert fake_gemini.requests == 1
    assert failure.application_failure_info.non_retryable, failure


@pytest.mark.parametrize("streaming", [False, True], ids=["invoke", "streaming"])
@pytest.mark.parametrize("status", [408, 429, 500, 503])
async def test_transient_status_stays_retryable(
    fake_gemini: _FakeGemini, status: int, streaming: bool
) -> None:
    fake_gemini.status = status
    failure = _recorded(await _run(streaming))
    assert failure.HasField("application_failure_info"), failure
    assert not failure.application_failure_info.non_retryable, failure
