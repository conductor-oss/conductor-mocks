#!/usr/bin/env python3
"""Normalize raw WireMock recordings into a stable, reviewable scenario.

The WireMock recorder captures real conductor-oss traffic verbatim, which
means every recording is full of one-shot noise: random stub UUIDs, that
run's execution and task ids, timestamps, worker ids, auth headers, and a
long tail of empty task polls. This script rewrites a raw recording into
the canonical form that lives in ``mocks/`` so that re-recording a scenario
produces a small, meaningful diff.

What it does, in order:

1. Orders mappings by the recorder's ``insertionIndex``.
2. Inlines JSON/text response bodies from ``__files`` (binary bodies are
   copied alongside the mappings instead).
3. Collapses consecutive empty task polls into a single stub.
4. Rewrites volatile identifiers to stable tokens: workflow/execution ids
   become ``EXEC_1``, ``EXEC_2``, …; task ids become ``TASK_1``, ….
5. Drops per-recording noise: stub UUIDs, timestamps, worker ids, response
   headers other than Content-Type.
6. Scrubs secrets: auth/cookie request headers are removed, token-like body
   values are replaced with ``"REDACTED"``.
7. Renames scenario states to a deterministic ``step_N`` chain.
8. Writes ``NN_<method>_<path>.json`` files under ``mocks/<scenario>/``.

Usage::

    python scripts/normalize.py <raw-recording-dir> --scenario agent/tool_happy_path

where ``<raw-recording-dir>`` is the WireMock root the recorder wrote into
(the directory containing ``mappings/`` and ``__files/``).
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Keys whose values identify a workflow execution / task. Conductor responses
# are not perfectly uniform, so both camelCase spellings seen in the wild are
# listed. Extend as new recordings surface new spellings.
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
    "scheduledTime",
    "callbackAfterSeconds",
    "workerId",
    "pollCount",
}

# Body keys whose values are secrets, regardless of nesting.
SECRET_BODY_KEYS = {"token", "apikey", "api_key", "keyid", "keysecret", "secret", "authorization"}

# Request headers never worth matching on — auth material and client noise.
SCRUBBED_REQUEST_HEADERS = {"authorization", "x-authorization", "cookie", "user-agent", "accept-encoding"}

EMPTY_POLL_PATH = re.compile(r"/tasks/poll/")


def _walk_json(node: Any) -> Iterable[Tuple[Dict[str, Any], str, Any]]:
    """Yield (parent_dict, key, value) for every dict entry, depth-first."""
    if isinstance(node, dict):
        for key, value in list(node.items()):
            yield node, key, value
            yield from _walk_json(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_json(item)


class Normalizer:
    def __init__(self, scenario: str):
        self.scenario = scenario
        self.id_map: Dict[str, str] = {}
        self._exec_count = 0
        self._task_count = 0

    # ── ID discovery and rewriting ────────────────────────────────────

    def collect_ids(self, body: Any) -> None:
        """Register execution/task ids found in a response body, in order."""
        for _, key, value in _walk_json(body):
            if not isinstance(value, str) or not value:
                continue
            if key in EXECUTION_ID_KEYS and value not in self.id_map:
                self._exec_count += 1
                self.id_map[value] = f"EXEC_{self._exec_count}"
            elif key in TASK_ID_KEYS and value not in self.id_map:
                self._task_count += 1
                self.id_map[value] = f"TASK_{self._task_count}"

    def rewrite_ids(self, text: str) -> str:
        """Replace every recorded id with its stable token, longest first."""
        for recorded in sorted(self.id_map, key=len, reverse=True):
            text = text.replace(recorded, self.id_map[recorded])
        return text

    # ── Per-mapping cleanup ───────────────────────────────────────────

    def clean_mapping(self, mapping: Dict[str, Any]) -> Dict[str, Any]:
        for field in ("id", "uuid", "insertionIndex", "persistent"):
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

    # ── Scenario state chain ──────────────────────────────────────────

    def renumber_states(self, mappings: List[Dict[str, Any]]) -> None:
        """Rewrite the recorder's generated state names to step_N, keeping
        the recorded order. WireMock requires the chain to begin at Started."""
        state_map: Dict[str, str] = {}
        step = 1
        scenario_name = self.scenario.replace("/", "_")
        for mapping in mappings:
            if "scenarioName" not in mapping and "newScenarioState" not in mapping:
                continue
            mapping["scenarioName"] = scenario_name
            required = mapping.get("requiredScenarioState")
            if required and required != "Started":
                mapping["requiredScenarioState"] = state_map.get(required, required)
            new = mapping.get("newScenarioState")
            if new:
                if new not in state_map:
                    state_map[new] = f"step_{step}"
                    step += 1
                mapping["newScenarioState"] = state_map[new]


def load_recording(raw_dir: Path) -> List[Tuple[Dict[str, Any], Optional[Path]]]:
    """Load recorder output, ordered by insertionIndex.

    Returns (mapping, body_file) pairs; body_file is the ``__files`` entry
    the mapping references, if any.
    """
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
    loaded.sort(key=lambda pair: pair[0].get("insertionIndex", 0))
    return loaded


def inline_body(mapping: Dict[str, Any], body_file: Optional[Path]) -> Optional[Path]:
    """Inline a __files body into the mapping when it is JSON or text.

    Returns the file path when it must be kept on disk (binary), else None.
    """
    if body_file is None or not body_file.exists():
        return None
    response = mapping["response"]
    raw = body_file.read_bytes()
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return body_file  # binary — keep as a file
    del response["bodyFileName"]
    try:
        response["jsonBody"] = json.loads(text)
    except json.JSONDecodeError:
        response["body"] = text  # e.g. an SSE event stream
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


def is_empty_poll(mapping: Dict[str, Any]) -> bool:
    """A task poll that returned no work — pure recording noise."""
    request = mapping.get("request", {})
    if request.get("method") != "GET":
        return False
    url = request.get("url") or request.get("urlPath") or ""
    if not EMPTY_POLL_PATH.search(url):
        return False
    response = mapping.get("response", {})
    body = response.get("body") or response.get("jsonBody")
    return body in (None, "", [], {})


def collapse_empty_polls(mappings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    collapsed: List[Dict[str, Any]] = []
    for mapping in mappings:
        if is_empty_poll(mapping) and collapsed and is_empty_poll(collapsed[-1]):
            # The dropped poll's outgoing state is what the next stub in the
            # chain requires — the surviving poll must adopt it, or replay
            # deadlocks waiting on a state nothing ever sets.
            if mapping.get("newScenarioState"):
                collapsed[-1]["newScenarioState"] = mapping["newScenarioState"]
            continue
        collapsed.append(mapping)
    return collapsed


def output_name(index: int, mapping: Dict[str, Any]) -> str:
    request = mapping.get("request", {})
    method = request.get("method", "any").lower()
    url = request.get("url") or request.get("urlPath") or request.get("urlPathTemplate") or ""
    slug = re.sub(r"[^a-z0-9]+", "_", url.split("?")[0].lower()).strip("_") or "root"
    return f"{index:02d}_{method}_{slug}.json"


def normalize(raw_dir: Path, scenario: str, out_root: Path) -> Path:
    normalizer = Normalizer(scenario)
    pairs = load_recording(raw_dir)

    kept_files: List[Tuple[Dict[str, Any], Path]] = []
    mappings: List[Dict[str, Any]] = []
    for mapping, body_file in pairs:
        kept = inline_body(mapping, body_file)
        parse_request_json(mapping)
        if kept is not None:
            kept_files.append((mapping, kept))
        mappings.append(mapping)

    mappings = collapse_empty_polls(mappings)

    for mapping in mappings:
        json_body = mapping.get("response", {}).get("jsonBody")
        if json_body is not None:
            normalizer.collect_ids(json_body)

    for mapping in mappings:
        normalizer.clean_mapping(mapping)
    normalizer.renumber_states(mappings)

    out_dir = out_root / scenario
    mappings_out = out_dir / "mappings"
    files_out = out_dir / "__files"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    mappings_out.mkdir(parents=True)
    files_out.mkdir()

    for index, mapping in enumerate(mappings, start=1):
        text = json.dumps(mapping, indent=2, ensure_ascii=False)
        text = normalizer.rewrite_ids(text)
        name = output_name(index, json.loads(text))
        (mappings_out / name).write_text(text + "\n")

    for mapping, body_file in kept_files:
        if mapping in mappings:
            shutil.copy2(body_file, files_out / body_file.name)
    if not any(files_out.iterdir()):
        files_out.rmdir()

    return out_dir


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("raw_dir", type=Path, help="WireMock root the recorder wrote into")
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
