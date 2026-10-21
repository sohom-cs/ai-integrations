"""CLA recovery timing and HTTP behavior without provider or GitHub access."""

from __future__ import annotations

import json
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

import cla_recheck as cla

REPO = "temporalio/ai-integrations"
SHA = "a" * 40
OPEN = {"state": "open", "head": {"sha": SHA}}


class Clock:
    def __init__(self):
        self.now = 0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        assert seconds > 0
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(cla, "time", clock)
    monkeypatch.setattr(cla, "_pr", lambda *_: OPEN)
    monkeypatch.setattr(cla, "_cla_status", lambda *_: None)
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    return clock


@pytest.mark.parametrize("state", ["success", "pending", "failure", "error"])
def test_existing_verdict_never_requests_a_recheck(clock, monkeypatch, state):
    monkeypatch.setattr(cla, "_cla_status", lambda *_: {"state": state})
    monkeypatch.setattr(cla, "_recheck", lambda *_: pytest.fail("Existing verdict must be left alone"))
    assert cla.retry_cla(REPO, 129, SHA) == 0
    assert clock.now == 30


def test_normal_webhook_can_complete_during_grace(clock, monkeypatch):
    monkeypatch.setattr(cla, "_cla_status", lambda *_: {"state": "success"} if clock.now >= 10 else None)
    monkeypatch.setattr(cla, "_recheck", lambda *_: pytest.fail("Normal webhook completed during grace"))
    assert cla.retry_cla(REPO, 129, SHA) == 0
    assert clock.now == 30


def test_next_attempt_checks_github_before_triggering_another_recheck(clock, monkeypatch):
    requests = []
    reads = []

    def status(*_):
        reads.append(clock.now)
        return {"state": "success"} if clock.now >= 40 else None

    monkeypatch.setattr(cla, "_recheck", lambda *_: requests.append(clock.now))
    monkeypatch.setattr(cla, "_cla_status", status)
    assert cla.retry_cla(REPO, 129, SHA) == 0
    assert requests == [30]
    assert reads == [30, 60]
    assert clock.now == 60


@pytest.mark.parametrize("unavailable", [False, True])
def test_backoff_also_applies_when_accepted_requests_never_post_a_status(clock, monkeypatch, unavailable):
    requests = []
    reads = []

    def status(*_):
        reads.append(clock.now)
        return None

    def recheck(*_):
        requests.append(clock.now)
        if unavailable:
            raise cla.RecheckUnavailable("HTTP 503: no healthy upstream")

    monkeypatch.setattr(cla, "_recheck", recheck)
    monkeypatch.setattr(cla, "_cla_status", status)
    assert cla.retry_cla(REPO, 129, SHA, timeout_seconds=1000) == 1
    assert requests == [30, 60, 120, 240, 480, 720, 960]
    assert reads == [30, 60, 120, 240, 480, 720, 960, 1000]
    assert clock.now == 1000


@pytest.mark.parametrize("state,head", [("closed", SHA), ("open", "b" * 40)])
def test_stops_when_the_pr_closes_or_the_head_changes(clock, monkeypatch, state, head):
    requests = []
    monkeypatch.setattr(cla, "_recheck", lambda *_: requests.append(clock.now))
    monkeypatch.setattr(cla, "_pr", lambda *_: OPEN if clock.now < 35 else {"state": state, "head": {"sha": head}})
    assert cla.retry_cla(REPO, 129, SHA) == 0
    assert requests == [30]
    assert clock.now == 60


@pytest.mark.parametrize("query", ["_pr", "_cla_status"])
def test_failed_github_reads_do_not_trigger_a_recheck(clock, monkeypatch, query):
    requests = []

    def read(*_):
        if 30 <= clock.now < 45:
            raise subprocess.CalledProcessError(1, ["gh", "api"])
        if query == "_pr":
            return OPEN
        return None

    monkeypatch.setattr(cla, query, read)
    monkeypatch.setattr(cla, "_recheck", lambda *_: requests.append(clock.now))
    assert cla.retry_cla(REPO, 129, SHA, timeout_seconds=65) == 1
    assert requests == [60]


def test_final_check_can_observe_a_result_without_sending_another_recheck(clock, monkeypatch):
    requests = []
    monkeypatch.setattr(cla, "_recheck", lambda *_: requests.append(clock.now))
    monkeypatch.setattr(cla, "_cla_status", lambda *_: {"state": "success"} if clock.now >= 40 else None)
    assert cla.retry_cla(REPO, 129, SHA, timeout_seconds=50) == 0
    assert requests == [30]
    assert clock.now == 50


def test_permanent_recheck_error_stops_without_retrying(clock, monkeypatch):
    requests = []

    def recheck(*_):
        requests.append(clock.now)
        raise cla.RecheckRejected("HTTP 403")

    monkeypatch.setattr(cla, "_recheck", recheck)
    assert cla.retry_cla(REPO, 129, SHA) == 1
    assert requests == [30]


def test_slow_github_query_cannot_start_a_recheck_after_the_deadline(clock, monkeypatch):
    def status(*_):
        clock.now = 100
        return None

    monkeypatch.setattr(cla, "_cla_status", status)
    monkeypatch.setattr(cla, "_recheck", lambda *_: pytest.fail("Retry window already expired"))
    assert cla.retry_cla(REPO, 129, SHA, timeout_seconds=60) == 1


def test_status_lookup_finds_the_latest_cla_even_beyond_the_first_page(monkeypatch):
    commands = []

    def run(command, **kwargs):
        commands.append(command)
        return SimpleNamespace(stdout=json.dumps([
            [{"context": "other", "state": "success"}],
            [{"context": "license/cla", "state": "pending"}, {"context": "license/cla", "state": "success"}],
        ]))

    monkeypatch.setattr(cla.subprocess, "run", run)
    assert cla._cla_status(REPO, SHA)["state"] == "pending"
    assert commands == [["gh", "api", f"repos/{REPO}/commits/{SHA}/statuses?per_page=100", "--paginate", "--slurp"]]


@pytest.fixture
def http_server(monkeypatch):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append((self.path, self.headers))
            # The PR number chooses the HTTP response for these transport tests.
            code = int(self.path.rsplit("=", 1)[1])
            self.send_response(code)
            if code == 302:
                self.send_header("Location", "/redirect-must-not-be-followed")
            self.end_headers()
            self.wfile.write(b"no healthy upstream" if code == 503 else b"response")

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01))
    thread.start()
    monkeypatch.setattr(cla, "CLA_URL", f"http://127.0.0.1:{server.server_port}/check")
    monkeypatch.setenv("GH_TOKEN", "github-token-must-not-reach-cla")
    try:
        yield requests
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


@pytest.mark.parametrize("code", [200, 302])
def test_http_acknowledgement_does_not_follow_redirects_or_send_github_token(http_server, code):
    cla._recheck(REPO, code)
    assert len(http_server) == 1
    path, headers = http_server[0]
    assert path == f"/check/{REPO}?pullRequest={code}"
    assert "Authorization" not in headers
    assert "github-token-must-not-reach-cla" not in str(headers)


@pytest.mark.parametrize("code,error", [(429, cla.RecheckUnavailable), (503, cla.RecheckUnavailable), (403, cla.RecheckRejected)])
def test_http_errors_distinguish_retryable_outages(http_server, code, error):
    with pytest.raises(error, match=f"HTTP {code}"):
        cla._recheck(REPO, code)
    assert len(http_server) == 1


def test_timeout_has_a_visible_summary(clock, monkeypatch, tmp_path):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    assert cla.retry_cla(REPO, 129, SHA, timeout_seconds=10) == 1
    assert "no license/cla result after 10s" in summary.read_text()


def test_workflow_starts_recovery_alongside_ci_with_read_only_permissions():
    workflow = yaml.safe_load((Path(__file__).parents[2] / ".github/workflows/ci.yml").read_text())
    job = workflow["jobs"]["cla-recheck"]
    assert "needs" not in job
    assert "cla-recheck" not in workflow["jobs"]["ci-status"]["needs"]
    assert job["permissions"] == {"contents": "read", "pull-requests": "read", "statuses": "read"}
    assert "github.event_name == 'pull_request'" in job["if"]
    for action in ("opened", "synchronize", "reopened", "ready_for_review"):
        assert f'"{action}"' in job["if"]
        assert action in workflow[True]["pull_request"]["types"]
    env = job["steps"][-1]["env"]
    assert env["PR_HEAD_SHA"] == "${{ github.event.pull_request.head.sha }}"
