#!/usr/bin/env python3
"""Normalize raw WireMock recordings into a stable, reviewable scenario.

Input is the output of WireMock's **snapshot recorder** (started via
``POST /__admin/recordings/start`` with ``repeatsAsScenarios: true`` and
``persist: true`` — NOT the legacy ``--record-mappings`` flag, which keeps
only one stub per unique URL and silently loses the task-bearing poll).

A raw recording is full of one-shot noise: random stub UUIDs, that run's
execution/task ids, timestamps, the recording machine's hostname in poll
query params, and a long chain of empty task polls. This script rewrites a
recording into the canonical form that lives in ``mocks/`` so a re-record
produces a small, meaningful diff — and so replay works on any machine.

What it does, in order:

1. Orders mappings chronologically (the snapshot listing is typically
   reverse-chronological; direction is inferred from the scenario chain).
2. Inlines JSON/text response bodies — both string ``body`` fields and
   ``__files`` references (binary bodies are copied alongside instead).
3. Reworks task polls for replay determinism: all empty polls become ONE
   low-priority catch-all stub matched on path only (no machine-specific
   ``workerid`` query param), and each task-bearing poll becomes a
   high-priority scenario step (``Started`` → ``step_1`` → …). Any number
   of empty polls at any point then replay correctly, with zero timing
   sensitivity.
4. Rewrites volatile identifiers to stable tokens: workflow/execution ids
   → ``EXEC_n``, task ids → ``TASK_n``, LLM tool-call ids → ``CALL_n``.
5. Drops per-recording noise: stub UUIDs, ``insertionIndex``, timestamps
   (including ``"timestamp":<millis>`` inside SSE text bodies), worker
   ids, response headers other than Content-Type.
6. Scrubs secrets: auth/cookie request headers are removed, token-like
   body values are replaced with ``"REDACTED"``.
7. Writes ``NN_<method>_<path>.json`` files under ``mocks/<scenario>/``.

Usage::

    python scripts/normalize.py <raw-recording-dir> --scenario agent/tool_happy_path

where ``<raw-recording-dir>`` is the WireMock root the recorder persisted
into (the directory containing ``mappings/`` and ``__files/``).
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Keys whose values identify a workflow execution / task. Extend as new
# recordings surface new spellings.
EXECUTION_ID_KEYS = {"workflowId", "workflowInstanceId", "executionId"}
TASK_ID_KEYS = {"taskId"}

# Per-recording noise removed from JSON bodies wherever it appears.
VOLATILE_BODY_KEYS = {
    "createTime",
    "createdTime",
    "updateTime",
    "updatedTime",
    "startTime",
    "endTime",
    "firstStartTime",
    "scheduledTime",
    "queueWaitTime",
    "callbackAfterSeconds",
    "workerId",
    "pollCount",
}

# Body keys whose values are secrets, regardless of nesting.
SECRET_BODY_KEYS = {"token", "apikey", "api_key", "keyid", "keysecret", "secret", "authorization"}

# Request headers never worth matching on — auth material and client noise.
SCRUBBED_REQUEST_HEADERS = {"authorization", "x-authorization", "cookie", "user-agent", "accept-encoding"}

POLL_PATH = re.compile(r"/tasks/poll/")
# Millisecond timestamps inside inlined text bodies (e.g. SSE data lines).
# Matches both raw and JSON-escaped quoting, since the rewrite runs over the
# serialized mapping where a text body's quotes appear as \".
TEXT_TIMESTAMP = re.compile(r'(\\?"timestamp\\?":)\d{10,}')
# LLM tool-call ids (e.g. OpenAI's call_9Idkpxx2xbJpwa1I0cU6puF4) leak into
# task reference names; rewrite them so only genuine changes diff.
CALL_ID = re.compile(r"call_[A-Za-z0-9]{8,}")

CATCH_ALL_POLL_PRIORITY = 10
TASK_POLL_PRIORITY = 1

# Replayed SSE streams are dribbled out over this duration instead of being
# delivered in one burst. Un-paced replay ends the run the instant `done`
# arrives — before the SDK's task worker has fired its first poll — so the
# tool never executes and the recorded task-update stub is never exercised.
# Pacing the stream restores the real concurrency window: poll → run the
# tool → POST the result (which must match the recorded body) → done.
SSE_DRIBBLE_MS = 3000


def _walk_json(node: Any) -> Iterable[Tuple[Dict[str, Any], str, Any]]:
    """Yield (parent_dict, key, value) for every dict entry, depth-first."""
    if isinstance(node, dict):
        for key, value in list(node.items()):
            yield node, key, value
            yield from _walk_json(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_json(item)


def _request_path(mapping: Dict[str, Any]) -> str:
    request = mapping.get("request", {})
    url = request.get("url") or request.get("urlPath") or request.get("urlPathTemplate") or ""
    return url.split("?")[0]


class Normalizer:
    def __init__(self, scenario: str):
        self.scenario = scenario
        self.id_map: Dict[str, str] = {}
        self._counters = {"EXEC": 0, "TASK": 0, "CALL": 0}

    # ── ID discovery and rewriting ────────────────────────────────────

    def _register(self, value: str, kind: str) -> None:
        if value and value not in self.id_map:
            self._counters[kind] += 1
            self.id_map[value] = f"{kind}_{self._counters[kind]}"

    def collect_ids(self, body: Any) -> None:
        """Register execution/task ids found in a response body, in order."""
        for _, key, value in _walk_json(body):
            if not isinstance(value, str):
                continue
            if key in EXECUTION_ID_KEYS:
                self._register(value, "EXEC")
            elif key in TASK_ID_KEYS:
                self._register(value, "TASK")

    def rewrite_text(self, text: str) -> str:
        """Apply all text-wide rewrites: ids, call ids, text timestamps."""
        for recorded in sorted(self.id_map, key=len, reverse=True):
            text = text.replace(recorded, self.id_map[recorded])
        for match in CALL_ID.findall(text):
            self._register(match, "CALL")
            text = text.replace(match, self.id_map[match])
        return TEXT_TIMESTAMP.sub(r"\g<1>0", text)

    # ── Per-mapping cleanup ───────────────────────────────────────────

    def clean_mapping(self, mapping: Dict[str, Any]) -> Dict[str, Any]:
        for field in ("id", "uuid", "name", "insertionIndex", "persistent"):
            mapping.pop(field, None)

        request = mapping.get("request", {})
        headers = request.get("headers")
        if headers:
            for name in list(headers):
                if name.lower() in SCRUBBED_REQUEST_HEADERS:
                    del headers[name]
            if not headers:
                del request["headers"]

        response = mapping.get("response", {})
        resp_headers = response.get("headers")
        if resp_headers:
            kept = {k: v for k, v in resp_headers.items() if k.lower() == "content-type"}
            if kept:
                response["headers"] = kept
            else:
                del response["headers"]

        for body in self._json_bodies(mapping):
            self._strip_volatile(body)
            self._redact_secrets(body)
        return mapping

    @staticmethod
    def _json_bodies(mapping: Dict[str, Any]) -> List[Any]:
        """Structured bodies inside a mapping: response jsonBody and request
        equalToJson patterns (parsed in place by the caller beforehand)."""
        bodies: List[Any] = []
        json_body = mapping.get("response", {}).get("jsonBody")
        if json_body is not None:
            bodies.append(json_body)
        for pattern in mapping.get("request", {}).get("bodyPatterns", []):
            parsed = pattern.get("equalToJson")
            if isinstance(parsed, (dict, list)):
                bodies.append(parsed)
        return bodies

    @staticmethod
    def _strip_volatile(body: Any) -> None:
        for parent, key, _ in list(_walk_json(body)):
            if key in VOLATILE_BODY_KEYS:
                parent.pop(key, None)

    @staticmethod
    def _redact_secrets(body: Any) -> None:
        for parent, key, value in _walk_json(body):
            if key.lower() in SECRET_BODY_KEYS and isinstance(value, str) and value:
                parent[key] = "REDACTED"


# ── Body inlining ─────────────────────────────────────────────────────


def parse_inline_bodies(mapping: Dict[str, Any], body_file: Optional[Path]) -> Optional[Path]:
    """Inline the response body as jsonBody (JSON) or body text.

    The snapshot recorder inlines small bodies as ``body`` strings and
    writes large ones to ``__files``. Either way, JSON becomes jsonBody so
    id collection and volatile-key stripping can walk it; non-JSON text
    (an SSE event stream) stays as body text. Returns the file path when
    the body is binary and must be kept on disk, else None.
    """
    response = mapping.get("response", {})
    text: Optional[str] = None
    if body_file is not None and body_file.exists():
        raw = body_file.read_bytes()
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            return body_file  # binary — keep as a file
        del response["bodyFileName"]
    elif isinstance(response.get("body"), str):
        text = response.pop("body")

    if text is not None:
        content_type = ""
        for name, value in (response.get("headers") or {}).items():
            if name.lower() == "content-type":
                content_type = str(value)
        if "json" in content_type:
            try:
                response["jsonBody"] = json.loads(text)
                return None
            except json.JSONDecodeError:
                pass
        response["body"] = text
        if "text/event-stream" in content_type:
            events = max(text.count("\nevent:"), 1)
            response["chunkedDribbleDelay"] = {
                "numberOfChunks": events * 2,
                "totalDuration": SSE_DRIBBLE_MS,
            }
    return None


def parse_request_json(mapping: Dict[str, Any]) -> None:
    """Parse string equalToJson patterns so cleanup can walk them."""
    for pattern in mapping.get("request", {}).get("bodyPatterns", []):
        value = pattern.get("equalToJson")
        if isinstance(value, str):
            try:
                pattern["equalToJson"] = json.loads(value)
            except json.JSONDecodeError:
                pass


# ── Ordering ──────────────────────────────────────────────────────────


def chronological(mappings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Order by insertionIndex, direction inferred from the scenario chain.

    The snapshot listing is typically reverse-chronological: the stub that
    requires ``Started`` (the chain head, i.e. the earliest request of its
    scenario) carries the highest insertionIndex. When that holds, sort
    descending; otherwise ascending.
    """
    ordered = sorted(mappings, key=lambda m: m.get("insertionIndex", 0))
    for name in {m.get("scenarioName") for m in mappings if m.get("scenarioName")}:
        chain = [m for m in ordered if m.get("scenarioName") == name]
        head = next((m for m in chain if m.get("requiredScenarioState") == "Started"), None)
        if head is not None and len(chain) > 1:
            if head is chain[-1]:
                return list(reversed(ordered))
            return ordered
    return ordered


# ── Poll rework ───────────────────────────────────────────────────────


def is_poll(mapping: Dict[str, Any]) -> bool:
    return bool(POLL_PATH.search(_request_path(mapping)))


def is_empty_poll(mapping: Dict[str, Any]) -> bool:
    """A task poll that returned no work — pure recording noise."""
    if not is_poll(mapping) or mapping.get("request", {}).get("method") != "GET":
        return False
    response = mapping.get("response", {})
    body = response.get("jsonBody", response.get("body"))
    return body in (None, "", [], {})


def _strip_poll_query(mapping: Dict[str, Any]) -> None:
    """Match polls on path only: the recorded query carries the recording
    machine's hostname (workerid), which never matches in CI."""
    request = mapping["request"]
    request["urlPath"] = _request_path(mapping)
    request.pop("url", None)
    request.pop("queryParameters", None)


def rework_polls(mappings: List[Dict[str, Any]], scenario: str) -> List[Dict[str, Any]]:
    """Replace the recorded poll chain with a timing-insensitive form.

    Per poll path: one catch-all empty-poll stub (low priority, stateless,
    matches any number of polls at any time) plus one scenario step per
    task-bearing poll (high priority, consumed in order: Started → step_1
    → …). Non-poll mappings pass through unchanged, in order.
    """
    scenario_name = scenario.replace("/", "_")
    result: List[Dict[str, Any]] = []
    seen_catch_all: set = set()
    step_by_path: Dict[str, int] = {}

    for mapping in mappings:
        if not is_poll(mapping):
            result.append(mapping)
            continue

        path = _request_path(mapping)
        _strip_poll_query(mapping)
        for field in ("scenarioName", "requiredScenarioState", "newScenarioState"):
            mapping.pop(field, None)

        if is_empty_poll(mapping):
            if path in seen_catch_all:
                continue
            seen_catch_all.add(path)
            mapping["priority"] = CATCH_ALL_POLL_PRIORITY
        else:
            step = step_by_path.get(path, 0) + 1
            step_by_path[path] = step
            mapping["priority"] = TASK_POLL_PRIORITY
            mapping["scenarioName"] = scenario_name
            mapping["requiredScenarioState"] = "Started" if step == 1 else f"step_{step - 1}"
            mapping["newScenarioState"] = f"step_{step}"
        result.append(mapping)
    return result


# ── Output ────────────────────────────────────────────────────────────


def output_name(index: int, mapping: Dict[str, Any]) -> str:
    method = mapping.get("request", {}).get("method", "any").lower()
    slug = re.sub(r"[^a-z0-9]+", "_", _request_path(mapping).lower()).strip("_") or "root"
    return f"{index:02d}_{method}_{slug}.json"


def load_recording(raw_dir: Path) -> List[Tuple[Dict[str, Any], Optional[Path]]]:
    """Load recorder output as (mapping, referenced __files entry) pairs."""
    mappings_dir = raw_dir / "mappings"
    if not mappings_dir.is_dir():
        sys.exit(f"error: {mappings_dir} not found — pass the WireMock root the recorder wrote into")

    loaded = []
    for path in sorted(mappings_dir.glob("*.json")):
        mapping = json.loads(path.read_text())
        body_file = None
        name = mapping.get("response", {}).get("bodyFileName")
        if name:
            body_file = raw_dir / "__files" / name
        loaded.append((mapping, body_file))
    return loaded


def normalize(raw_dir: Path, scenario: str, out_root: Path) -> Path:
    normalizer = Normalizer(scenario)

    binary_files: List[Tuple[Dict[str, Any], Path]] = []
    mappings: List[Dict[str, Any]] = []
    for mapping, body_file in load_recording(raw_dir):
        kept = parse_inline_bodies(mapping, body_file)
        parse_request_json(mapping)
        if kept is not None:
            binary_files.append((mapping, kept))
        mappings.append(mapping)

    mappings = chronological(mappings)
    mappings = rework_polls(mappings, scenario)

    for mapping in mappings:
        json_body = mapping.get("response", {}).get("jsonBody")
        if json_body is not None:
            normalizer.collect_ids(json_body)

    for mapping in mappings:
        normalizer.clean_mapping(mapping)

    out_dir = out_root / scenario
    mappings_out = out_dir / "mappings"
    files_out = out_dir / "__files"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    mappings_out.mkdir(parents=True)
    files_out.mkdir()

    for index, mapping in enumerate(mappings, start=1):
        text = normalizer.rewrite_text(json.dumps(mapping, indent=2, ensure_ascii=False))
        name = output_name(index, json.loads(text))
        (mappings_out / name).write_text(text + "\n")

    for mapping, body_file in binary_files:
        if mapping in mappings:
            shutil.copy2(body_file, files_out / body_file.name)
    if not any(files_out.iterdir()):
        files_out.rmdir()

    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("raw_dir", type=Path, help="WireMock root the recorder persisted into")
    parser.add_argument("--scenario", required=True, help="catalog path, e.g. agent/tool_happy_path")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "mocks",
        help="catalog root to write into (default: mocks/)",
    )
    args = parser.parse_args()

    out_dir = normalize(args.raw_dir, args.scenario, args.out)
    count = len(list((out_dir / "mappings").glob("*.json")))
    print(f"wrote {count} mappings to {out_dir}")
    print("review the diff before opening a PR — especially for un-scrubbed secrets")


if __name__ == "__main__":
    main()
