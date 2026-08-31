"""Behavior tests for scripts/normalize.py.

The fixture mirrors real WireMock snapshot-recorder output captured from a
live conductor-oss run of the agent/tool_happy_path flow (weather bot over
SSE): reverse-chronological insertionIndex, string response bodies,
machine-specific workerid query params on polls, a recorded poll scenario
chain with empty polls around one task-bearing poll, and millisecond
timestamps inside the SSE text body.

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

WF_ID = "776db26f-0780-4d5c-b486-d1aa99b1684d"
TASK_ID = "16ad71bc-4684-4d59-bc77-eeba06bdbb8a"
CALL_ID = "call_ifnzrHFiYQLozwDd5tgCNMaA"

SSE_BODY = (
    ":connected\n\n"
    f'id:1\nevent:tool_call\ndata:{{"id":1,"type":"tool_call","executionId":"{WF_ID}",'
    f'"toolName":"get_weather","args":{{"city":"Lisbon","units":"metric"}},"timestamp":1788206337860}}\n\n'
    'id:2\nevent:done\ndata:{"id":2,"type":"done","output":{"result":"Sunny in Lisbon, 21C."},'
    '"timestamp":1788206338000}\n\n'
)


def _poll(index, required, new, task=False):
    m = {
        "id": f"stub-{index}",
        "uuid": f"stub-{index}",
        "name": "api_tasks_poll_batch_get_weather",
        "persistent": True,
        "insertionIndex": index,
        "request": {
            "urlPath": "/api/tasks/poll/batch/get_weather",
            "method": "GET",
            "queryParameters": {
                "workerid": {"hasExactly": [{"equalTo": "fedorabox"}]},
                "count": {"hasExactly": [{"equalTo": "1"}]},
                "timeout": {"hasExactly": [{"equalTo": "100"}]},
            },
        },
        "response": {
            "status": 200,
            "body": "[]",
            "headers": {"Content-Type": "application/json", "Keep-Alive": "timeout=60"},
        },
        "scenarioName": "scenario-1-api-tasks-poll-batch-get_weather",
        "requiredScenarioState": required,
    }
    if new:
        m["newScenarioState"] = new
    if task:
        m["response"]["body"] = json.dumps([{
            "taskType": "get_weather",
            "status": "IN_PROGRESS",
            "inputData": {"city": "Lisbon", "units": "metric"},
            "referenceTaskName": f"{CALL_ID}_0__1",
            "taskId": TASK_ID,
            "workflowInstanceId": WF_ID,
            "workerId": "fedorabox",
            "pollCount": 1,
            "scheduledTime": 1788206337754,
            "queueWaitTime": 74,
        }])
    return m


def _plain(index, method, url, response, request_json=None):
    m = {
        "id": f"stub-{index}",
        "uuid": f"stub-{index}",
        "name": "x",
        "persistent": True,
        "insertionIndex": index,
        "request": {"url": url, "method": method},
        "response": response,
    }
    if request_json is not None:
        m["request"]["bodyPatterns"] = [{
            "equalToJson": json.dumps(request_json),
            "ignoreArrayOrder": True,
            "ignoreExtraElements": True,
        }]
    return m


def build_fixture(root: Path):
    """Reverse-chronological insertionIndex, like a real snapshot listing."""
    mappings_dir = root / "mappings"
    files_dir = root / "__files"
    mappings_dir.mkdir(parents=True)
    files_dir.mkdir()

    big_status = {"executionId": WF_ID, "status": "COMPLETED", "startTime": 1788206183167,
                  "output": {"result": "Sunny in Lisbon, 21C."},
                  "tokenUsage": {"totalTokens": 250}}
    (files_dir / "execution.json").write_text(json.dumps(big_status))

    raw = [
        # chronological order: start(9) → taskdefs(8) → polls(7,6) task(5) → update(4) → polls(3,2) → stream + status
        _plain(9, "POST", "/api/agent/start",
               {"status": 200,
                "body": json.dumps({"executionId": WF_ID, "agentName": "weather",
                                    "requiredWorkers": ["get_weather"]}),
                "headers": {"Content-Type": "application/json", "Date": "Mon, 31 Aug 2026"}},
               request_json={"agentConfig": {"name": "weather", "model": "openai/gpt-4o-mini"},
                             "prompt": "Weather in Lisbon?"}),
        _plain(8, "PUT", "/api/metadata/taskdefs",
               {"status": 200, "headers": {"Keep-Alive": "timeout=60"}},
               request_json={"name": "get_weather", "retryCount": 2}),
        _poll(7, "Started", "s-2"),
        _poll(6, "s-2", "s-3"),
        _poll(5, "s-3", "s-4", task=True),
        _plain(4, "POST", "/api/tasks/update-v2", {"status": 204},
               request_json={"workflowInstanceId": WF_ID, "taskId": TASK_ID,
                             "workerId": "agent-sdk", "status": "COMPLETED",
                             "outputData": {"temp_c": 21.0, "summary": "Sunny in Lisbon"},
                             "token": "abc123secret"}),
        _poll(3, "s-4", "s-5"),
        _poll(2, "s-5", None),
        _plain(1, "GET", f"/api/agent/stream/{WF_ID}",
               {"status": 200, "body": SSE_BODY,
                "headers": {"Content-Type": "text/event-stream"}}),
        _plain(0, "GET", f"/api/agent/execution/{WF_ID}",
               {"status": 200, "bodyFileName": "execution.json",
                "headers": {"Content-Type": "application/json",
                            "Authorization": "Bearer sk-live-REAL-KEY"}}),
    ]
    for m in raw:
        (mappings_dir / f"mapping-{m['insertionIndex']}.json").write_text(json.dumps(m))


class NormalizeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp())
        raw = cls.tmp / "recording"
        build_fixture(raw)
        cls.out_dir = normalize(raw, "agent/tool_happy_path", cls.tmp / "mocks")
        cls.names = sorted(p.name for p in (cls.out_dir / "mappings").glob("*.json"))
        cls.outputs = [json.loads((cls.out_dir / "mappings" / n).read_text()) for n in cls.names]

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp)

    def _all_text(self):
        return json.dumps(self.outputs)

    def _polls(self):
        return [m for m in self.outputs if "/tasks/poll/" in m["request"].get("urlPath", "")]

    def test_polls_become_catch_all_plus_scenario_steps(self):
        polls = self._polls()
        # 5 recorded polls → 1 catch-all empty + 1 task step
        self.assertEqual(len(polls), 2)
        catch_all = [m for m in polls if m.get("priority") == 10]
        task_polls = [m for m in polls if m.get("priority") == 1]
        self.assertEqual(len(catch_all), 1)
        self.assertEqual(len(task_polls), 1)
        self.assertNotIn("scenarioName", catch_all[0])
        self.assertEqual(task_polls[0]["scenarioName"], "agent_tool_happy_path")
        self.assertEqual(task_polls[0]["requiredScenarioState"], "Started")
        self.assertEqual(task_polls[0]["newScenarioState"], "step_1")

    def test_polls_match_on_path_only(self):
        for m in self._polls():
            self.assertNotIn("queryParameters", m["request"])
            self.assertNotIn("url", m["request"])
            self.assertEqual(m["request"]["urlPath"], "/api/tasks/poll/batch/get_weather")
        self.assertNotIn("fedorabox", self._all_text())

    def test_chronological_output_despite_reversed_indices(self):
        self.assertEqual(self.names[0], "01_post_api_agent_start.json")
        self.assertTrue(self.names[-1].endswith("_get_api_agent_execution_exec_1.json"))

    def test_rewrites_ids_everywhere(self):
        text = self._all_text()
        for recorded in (WF_ID, TASK_ID, CALL_ID):
            self.assertNotIn(recorded, text)
        self.assertIn("EXEC_1", text)
        self.assertIn("TASK_1", text)
        self.assertIn("CALL_1", text)
        stream = next(m for m in self.outputs if "stream" in m["request"].get("url", ""))
        self.assertEqual(stream["request"]["url"], "/api/agent/stream/EXEC_1")
        self.assertIn("EXEC_1", stream["response"]["body"])

    def test_sse_stream_is_paced_on_replay(self):
        stream = next(m for m in self.outputs if "stream" in m["request"].get("url", ""))
        dribble = stream["response"]["chunkedDribbleDelay"]
        self.assertGreater(dribble["totalDuration"], 0)
        self.assertGreaterEqual(dribble["numberOfChunks"], 2)
        # only event-streams are paced
        for m in self.outputs:
            if m is not stream:
                self.assertNotIn("chunkedDribbleDelay", m.get("response", {}))

    def test_scrubs_timestamps_in_text_bodies(self):
        stream = next(m for m in self.outputs if "stream" in m["request"].get("url", ""))
        self.assertNotIn("1788206337860", stream["response"]["body"])
        self.assertIn('"timestamp":0', stream["response"]["body"])

    def test_inlines_string_and_file_bodies_as_json(self):
        start = next(m for m in self.outputs if m["request"].get("url") == "/api/agent/start")
        self.assertEqual(start["response"]["jsonBody"]["requiredWorkers"], ["get_weather"])
        status = next(m for m in self.outputs if "execution" in m["request"].get("url", ""))
        self.assertNotIn("bodyFileName", status["response"])
        self.assertEqual(status["response"]["jsonBody"]["status"], "COMPLETED")

    def test_scrubs_secrets_and_noise(self):
        text = self._all_text()
        self.assertNotIn("sk-live-REAL-KEY", text)
        self.assertNotIn("abc123secret", text)
        for noise in ("workerId\": \"fedorabox", "pollCount", "scheduledTime", "queueWaitTime",
                      "Keep-Alive", "Date", "insertionIndex", "persistent"):
            self.assertNotIn(noise, text)

    def test_preserves_tool_result_body_match(self):
        post = next(m for m in self.outputs if m["request"].get("url") == "/api/tasks/update-v2")
        body = post["request"]["bodyPatterns"][0]["equalToJson"]
        self.assertEqual(body["outputData"]["summary"], "Sunny in Lisbon")
        self.assertEqual(body["taskId"], "TASK_1")
        self.assertEqual(body["workflowInstanceId"], "EXEC_1")
        # workerId is stripped from the match pattern: SDKs send different
        # worker ids, and ignoreExtraElements makes the looser match safe.
        self.assertNotIn("workerId", body)


if __name__ == "__main__":
    unittest.main()
