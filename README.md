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

1. Start conductor-oss locally (e.g. `:8080`) with your LLM provider
   configured.
2. Start WireMock as a recording proxy in front of it:

   ```bash
   docker run -it --rm -p 9999:8080 -v $PWD/recording:/home/wiremock \
     wiremock/wiremock:3x \
     --record-mappings --proxy-all=http://host.docker.internal:8080
   ```

3. Run the SDK test/example against the proxy
   (`CONDUCTOR_SERVER_URL=http://localhost:9999/api`). The agent genuinely
   runs: real compile, real provider calls, real tool execution.
4. Normalize and file the captured mappings:

   ```bash
   python scripts/normalize.py recording --scenario agent/tool_happy_path
   ```

   The normalizer rewrites volatile identifiers (`wf_8f3a` → `EXEC_1`,
   task ids → `TASK_1`), drops timestamps and per-recording noise, collapses
   empty polls, scrubs auth material, and files the result under
   `mocks/agent/tool_happy_path/`.

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

- Ordered behavior is enforced with WireMock **scenarios** (recorded via
  `repeatsAsScenarios`): a stub only matches in the recorded order.
- The tool assertion is structural: the recorded `POST /tasks` stub only
  matches the recorded body — a wrong tool result matches nothing and the
  run fails.
- WireMock has no native SSE streaming. That is irrelevant here: an agent's
  event stream terminates at `done`, so the captured body is complete and
  replay delivers it un-paced.
