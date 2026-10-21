"""Recover missing CLA Assistant statuses alongside PR CI, without publishing statuses.

The public recheck endpoint redirects before processing finishes. Only a
license/cla status on the expected PR head confirms that it produced a result.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

CLA_URL = "https://cla-assistant.io/check"
CONTEXT = "license/cla"
GRACE_SECONDS = 30
MAX_BACKOFF_SECONDS = 240
TIMEOUT_SECONDS = 20 * 60
REQUEST_TIMEOUT_SECONDS = 20


class RecheckUnavailable(Exception):
    """A transport failure or transient HTTP response that can be retried."""


class RecheckRejected(Exception):
    """An HTTP response requiring intervention instead of another retry."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Inspect the recheck response without following its redirect to GitHub."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _github(path: str, *, paginate: bool = False):
    command = ["gh", "api", path]
    if paginate:
        command.extend(["--paginate", "--slurp"])
    return json.loads(subprocess.run(
        command, check=True, capture_output=True, text=True, timeout=REQUEST_TIMEOUT_SECONDS,
    ).stdout)


def _pr(repo: str, number: int) -> dict:
    return _github(f"repos/{repo}/pulls/{number}")


def _cla_status(repo: str, sha: str) -> dict | None:
    # Status history is newest first. Read every page so a busy commit's other
    # integrations cannot hide license/cla beyond the first page.
    pages = _github(f"repos/{repo}/commits/{sha}/statuses?per_page=100", paginate=True)
    return next((status for page in pages for status in page if status["context"] == CONTEXT), None)


def _recheck(repo: str, number: int) -> None:
    # This is a public endpoint: never send GH_TOKEN or an Authorization header.
    request = urllib.request.Request(
        f"{CLA_URL}/{repo}?pullRequest={number}",
        headers={"User-Agent": "temporalio-ai-integrations-cla-recheck"},
    )
    try:
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=REQUEST_TIMEOUT_SECONDS):
            pass
    except urllib.error.HTTPError as error:
        error.close()
        if 300 <= error.code < 400:
            return  # Acknowledgement only; verify GitHub on the next attempt.
        if error.code == 429 or 500 <= error.code < 600:
            raise RecheckUnavailable(f"CLA Assistant returned HTTP {error.code}") from error
        raise RecheckRejected(f"CLA Assistant returned HTTP {error.code}") from error
    except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
        raise RecheckUnavailable(f"CLA Assistant could not be reached: {error}") from error


def _finish(message: str, *, failed: bool = False) -> int:
    print(f"::warning::{message}" if failed else message, flush=True)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a") as output:
            output.write(f"### CLA recheck\n\n{message}\n\n")
    return int(failed)


def retry_cla(repo: str, number: int, sha: str, *, timeout_seconds: int = TIMEOUT_SECONDS) -> int:
    """Check GitHub before each CLA recheck, with capped exponential backoff."""
    deadline = time.monotonic() + timeout_seconds
    backoff = GRACE_SECONDS
    attempts = 0
    print(f"Watching {repo}#{number} at {sha}; first recheck after {GRACE_SECONDS}s.", flush=True)
    time.sleep(min(GRACE_SECONDS, timeout_seconds))

    while True:
        try:
            pr = _pr(repo, number)
            if pr["state"] != "open":
                return _finish(f"PR #{number} closed; stopped CLA recovery.")
            if pr["head"]["sha"] != sha:
                return _finish(f"PR #{number} has a newer head; its CI run handles CLA recovery.")
            status = _cla_status(repo, sha)
            if status:
                # Pending means unsigned in CLA Assistant. All existing verdicts
                # belong to the provider; this helper only recovers missing ones.
                return _finish(f"PR #{number}: {CONTEXT} reported {status['state']} after {attempts} recheck(s).")

            if time.monotonic() >= deadline:
                break
            attempts += 1
            print(f"Recheck {attempts}: no CLA result on {sha}.", flush=True)
            try:
                _recheck(repo, number)
            except RecheckUnavailable as error:
                print(str(error), flush=True)
            except RecheckRejected as error:
                return _finish(str(error), failed=True)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as error:
            # An unreadable GitHub result is not evidence of an absent CLA.
            # Do not call the recheck endpoint until both reads succeed.
            print(f"GitHub query failed: {error}", flush=True)

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        # An accepted redirect may never produce a status. Verify it at the
        # next attempt, including one final read when the retry window ends.
        time.sleep(min(backoff, remaining))
        backoff = min(backoff * 2, MAX_BACKOFF_SECONDS)

    return _finish(
        f"PR #{number}: no {CONTEXT} result after {timeout_seconds}s and {attempts} recheck(s). "
        "Retry CI or use CLA Assistant's recheck link when the service recovers.",
        failed=True,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--pr", required=True, type=int)
    parser.add_argument("--sha", required=True)
    args = parser.parse_args(argv)
    if not args.repo or not re.fullmatch(r"[\w.-]+/[\w.-]+", args.repo):
        parser.error("--repo must be owner/name")
    if args.pr <= 0 or not re.fullmatch(r"[0-9a-f]{40}", args.sha):
        parser.error("--pr must be positive and --sha must be a full commit SHA")
    return retry_cla(args.repo, args.pr, args.sha)


if __name__ == "__main__":
    raise SystemExit(main())
