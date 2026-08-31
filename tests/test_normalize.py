"""Behavior tests for scripts/normalize.py against a synthetic recording.

The fixture mimics the shape of raw WireMock recorder output for the
agent/tool_happy_path flow: start, SSE stream, empty polls, a real poll,
a task result POST, and a status check. Real recordings (Phase 3 of the
weather-bot plan) will replace assumptions here as they surface — these
tests pin the normalizer's contract, not conductor's exact payloads.

Run with: python -m unittest discover tests
"""

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from normalize import normalize  # noqa: E402

WF_ID = "8f3a1b2c-aaaa-bbbb-cccc-000000000001"
TASK_ID = "91d0e5f6-dddd-eeee-ffff-000000000002"


def _mapping(index, method, url, response, scenario_state=None, new_state=None, body_json=None):
    m = {
        "id": f"stub-{index}",
        "uuid": f"stub-{index}",
        "insertionIndex": index,
        "persistent": True,
        "request": {
            "method": method,
            "url": url,
            "headers": {
                "Authorization": {"equalTo": "Bearer sk-live-REAL-KEY"},
                "Accept": {"equalTo": "application/json"},
            },
        },
        "response": response,
    }
    if body_json is not None:
        m["request"]["bodyPatterns"] = [{"equalToJson": json.dumps(body_json)}]
    if scenario_state or new_state:
        m["scenarioName"] = "recorded-scenario-1"
        m["requiredScenarioState"] = scenario_state or "Started"
        if new_state:
            m["newScenarioState"] = new_state
    return m


def build_fixture(root: Path):
    mappings = root / "mappings"
    files = root / "__files"
    mappings.mkdir(parents=True)
    files.mkdir()

    (files / "stream.txt").write_text(
        f'event: tool_call\ndata: {{"toolName": "get_weather", "executionId": "{WF_ID}"}}\n\n'
        "event: done\ndata: {\"output\": \"Sunny in Lisbon, 21C.\"}\n\n"
    )

    raw = [
        _mapping(
            0, "POST", "/api/agent/start",
            {
                "status": 200,
                "headers": {"Content-Type": "application/json", "Date": "Mon, 31 Aug 2026"},
                "jsonBody": {
                    "executionId": WF_ID,
                    "createTime": 1756640000000,
                    "requiredWorkers": ["get_weather"],
                },
            },
            scenario_state="Started", new_state="scenario-1-start-2",
        ),
        _mapping(
            1, "GET", f"/api/agent/stream/{WF_ID}",
            {
                "status": 200,
                "headers": {"Content-Type": "text/event-stream"},
                "bodyFileName": "stream.txt",
            },
        ),
        # two consecutive empty polls, then the real one
        _mapping(2, "GET", "/api/tasks/poll/get_weather", {"status": 200, "body": ""},
                 scenario_state="scenario-1-start-2", new_state="scenario-1-poll-2"),
        _mapping(3, "GET", "/api/tasks/poll/get_weather", {"status": 200, "body": ""},
                 scenario_state="scenario-1-poll-2", new_state="scenario-1-poll-3"),
        _mapping(
            4, "GET", "/api/tasks/poll/get_weather",
            {
                "status": 200,
                "headers": {"Content-Type": "application/json"},
                "jsonBody": {
                    "taskId": TASK_ID,
                    "workflowInstanceId": WF_ID,
                    "workerId": "nicks-laptop",
                    "pollCount": 3,
                    "inputData": {"city": "Lisbon"},
                },
            },
            scenario_state="scenario-1-poll-3", new_state="scenario-1-polled-4",
        ),
        _mapping(
            5, "POST", "/api/tasks",
            {"status": 200, "jsonBody": {"status": "ok", "token": "abc123secret"}},
            scenario_state="scenario-1-polled-4", new_state="scenario-1-done-5",
            body_json={
                "taskId": TASK_ID,
                "workflowInstanceId": WF_ID,
                "outputData": {"temp_c": 21.0, "summary": "Sunny in Lisbon"},
            },
        ),
    ]
    for m in raw:
        (mappings / f"mapping-{m['insertionIndex']}.json").write_text(json.dumps(m))


class NormalizeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp())
        raw = cls.tmp / "recording"
        build_fixture(raw)
        cls.out_dir = normalize(raw, "agent/tool_happy_path", cls.tmp / "mocks")
        cls.outputs = {
            p.name: json.loads(p.read_text()) for p in sorted((cls.out_dir / "mappings").glob("*.json"))
        }

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp)

    def _all_text(self):
        return json.dumps(list(self.outputs.values()))

    def test_collapses_consecutive_empty_polls(self):
        # 6 recorded mappings, 2 of the 3 empty polls collapsed into 1 → 5 files
        self.assertEqual(len(self.outputs), 5)

    def test_rewrites_ids_everywhere(self):
        text = self._all_text()
        self.assertNotIn(WF_ID, text)
        self.assertNotIn(TASK_ID, text)
        self.assertIn("EXEC_1", text)
        self.assertIn("TASK_1", text)
        # including inside the inlined SSE body and request URLs
        stream = next(m for m in self.outputs.values() if "stream" in m["request"]["url"])
        self.assertEqual(stream["request"]["url"], "/api/agent/stream/EXEC_1")
        self.assertIn("EXEC_1", stream["response"]["body"])

    def test_inlines_bodies(self):
        for m in self.outputs.values():
            self.assertNotIn("bodyFileName", m["response"])
        start = next(m for m in self.outputs.values() if m["request"]["url"] == "/api/agent/start")
        self.assertEqual(start["response"]["jsonBody"]["requiredWorkers"], ["get_weather"])

    def test_scrubs_secrets_and_noise(self):
        text = self._all_text()
        self.assertNotIn("sk-live-REAL-KEY", text)
        self.assertNotIn("abc123secret", text)
        self.assertNotIn("nicks-laptop", text)
        for noise in ("createTime", "workerId", "pollCount", "Authorization", "Date"):
            self.assertNotIn(f'"{noise}"', text)

    def test_strips_stub_noise_fields(self):
        for m in self.outputs.values():
            for field in ("id", "uuid", "insertionIndex", "persistent"):
                self.assertNotIn(field, m)

    def test_deterministic_scenario_states(self):
        named = [m for m in self.outputs.values() if "scenarioName" in m]
        self.assertTrue(named)
        for m in named:
            self.assertEqual(m["scenarioName"], "agent_tool_happy_path")
            for state in (m.get("requiredScenarioState"), m.get("newScenarioState")):
                if state:
                    self.assertTrue(
                        state == "Started" or state.startswith("step_"),
                        f"unexpected state name: {state}",
                    )

    def test_preserves_tool_result_body_match(self):
        post = next(m for m in self.outputs.values() if m["request"]["method"] == "POST"
                    and m["request"]["url"] == "/api/tasks")
        body = post["request"]["bodyPatterns"][0]["equalToJson"]
        self.assertEqual(body["outputData"]["summary"], "Sunny in Lisbon")
        self.assertEqual(body["taskId"], "TASK_1")

    def test_output_files_are_ordered_and_named(self):
        names = sorted(self.outputs)
        self.assertEqual(names[0], "01_post_api_agent_start.json")
        self.assertTrue(all(n[:2].isdigit() for n in names))


if __name__ == "__main__":
    unittest.main()
