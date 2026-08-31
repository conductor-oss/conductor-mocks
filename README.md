# conductor-mocks

Recorded [Conductor](https://github.com/conductor-oss/conductor) server
interactions for SDK tests — agentic and plain-workflow alike.

The mocking layer is **SDK-agnostic and shared across every Conductor SDK**.
WireMock records a real conductor-oss server once; WireMock replays it in CI
everywhere. No SDK ships a custom mock server, and no SDK internals are ever
stubbed.

Source of truth for the testing strategy:
[`ruby-sdk/docs/design/AGENT_TESTING.md`](https://github.com/conductor-oss/ruby-sdk/blob/main/docs/design/AGENT_TESTING.md).

## The model: record once, every SDK replays

Recordings live here as a catalog of **named scenarios** — one WireMock
mappings directory per scenario. A test in any SDK references a scenario by
name: the Ruby weather test, the Python one, and a future Go one all boot
`agent/tool_happy_path` and assert their own expectations.

Replay matches by verb + path + order, so one recording works for every SDK
exactly when the SDKs send equivalent requests — the parity contract.
Replaying a recording another SDK produced is itself a behavioral parity test.

```
mocks/
  agent/
    tool_happy_path/        # LLM calls get_weather(city: "Lisbon"), answers from the result
    approval_approve/       # (planned)
    approval_reject/        # (planned)
    secrets_runtime_metadata/  # (planned)
    team_handoff/           # (planned)
    mcp_discovery/          # (planned)
  workflow/
    simple_task/            # (planned)
    dynamic_fork/           # (planned)
```

Each scenario directory is a WireMock root: `mappings/` (the stubs) and
`__files/` (response bodies, when not inlined).

## Ground rules

- **Nothing replay serves was invented by hand.** Every mapping was recorded
  from a real conductor-oss. Hand-edited response bodies are not accepted;
  if a scenario is wrong, re-record it.
- **Re-recording is always manual** and shows up as a reviewable diff in this
  repo. PRs are reviewed and merged by the conductor-oss team.
- **CI starts no Conductor and holds no provider keys.** An unmatched request
  fails the run loudly.
- **Recordings must never contain secrets.** The normalizer scrubs auth
  headers and token-like values; review diffs for anything it missed.
- **Assert structure, not prose.** Recorded model text is that day's LLM
  output. SDK tests should assert `finish_reason`, tool names, tool args,
  and tool results — not exact response wording.

## Recording a scenario

Recording is manual, local, and real everything: your own conductor-oss, real
provider integrations, a real LLM. Nothing is spun up for you.

Use WireMock's **snapshot recorder** (`/__admin/recordings`), NOT the legacy
`--record-mappings` flag — the legacy recorder keeps one stub per unique URL,
so the task-bearing poll response is silently lost among the empty polls that
share its URL. The snapshot recorder captures every request, with repeats as
an ordered scenario chain.

1. Start conductor-oss locally (e.g. `:8080`) with your LLM provider
   configured.
2. Start WireMock and begin recording through it as a proxy:

   ```bash
   docker run -d --name recorder -p 9999:8080 \
     --add-host=host.docker.internal:host-gateway \
     -v $PWD/recording:/home/wiremock:Z \
     wiremock/wiremock:3x
   curl -X POST http://localhost:9999/__admin/recordings/start \
     -H 'Content-Type: application/json' \
     -d '{"targetBaseUrl": "http://host.docker.internal:8080",
          "repeatsAsScenarios": true, "persist": true,
          "extractBodyCriteria": {"textSizeThreshold": "2048",
                                  "binarySizeThreshold": "1"}}'
   ```

   (The `:Z` mount flag matters on SELinux hosts — without it WireMock
   silently persists nothing.)

3. Run the SDK test/example against the proxy
   (`CONDUCTOR_SERVER_URL=http://localhost:9999/api`). The agent genuinely
   runs: real compile, real provider calls, real tool execution.
4. Stop recording, then normalize and file the captured mappings:

   ```bash
   curl -X POST http://localhost:9999/__admin/recordings/stop
   docker stop recorder && docker rm recorder
   python scripts/normalize.py recording --scenario agent/tool_happy_path
   ```

   The normalizer rewrites volatile identifiers (execution ids → `EXEC_1`,
   task ids → `TASK_1`, LLM call ids → `CALL_1`), drops timestamps and
   per-recording noise, scrubs auth material, reworks the poll chain for
   machine-independent replay, paces the SSE stream, and files the result
   under `mocks/agent/tool_happy_path/`.

5. Open a PR. The diff is the review artifact.

## Replaying in CI

Each SDK's CI checks out this repo alongside the SDK repo and starts WireMock
from the scenario directory — the official image, no packaging:

```yaml
- uses: actions/checkout@v4
  with: { repository: conductor-oss/conductor-mocks, path: conductor-mocks }
- run: docker run -d -p 8080:8080
         -v $PWD/conductor-mocks/mocks/agent/tool_happy_path:/home/wiremock
         wiremock/wiremock:3x
- run: CONDUCTOR_SERVER_URL=http://localhost:8080/api <run the SDK's replay tests>
```

Notes on replay behavior:

- Task polls replay machine-independently: the normalizer strips the
  recorded query params (the poll `workerid` is the recording machine's
  hostname) and splits polls into one stateless low-priority catch-all for
  empty responses plus one high-priority scenario step per task-bearing
  poll (`Started` → `step_1` → …). Any number of polls at any time replays
  correctly.
- The tool assertion is structural: the recorded task-update stub
  (`POST /api/tasks/update-v2`) only matches the recorded body — a wrong
  tool result matches nothing and the run fails. Verify it in tests via
  the request journal (`GET /__admin/requests`), and assert
  `GET /__admin/requests/unmatched` is empty.
- WireMock has no native SSE streaming, and the captured stream body is
  complete (an agent's stream terminates at `done`) — but replaying it
  **un-paced is not enough**: `done` then arrives before the SDK's task
  worker fires its first poll, the runtime shuts down, and the tool never
  executes — the run goes green without ever exercising the task-update
  stub. The normalizer therefore paces event-stream responses with a
  `chunkedDribbleDelay` (~3s), restoring the concurrency window in which
  the tool genuinely runs. (Verified empirically with the python-sdk.)
