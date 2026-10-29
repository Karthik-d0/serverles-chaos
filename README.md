# serverless-chaos

Adaptive, safe chaos engineering for **event-driven serverless applications** (Lambda, SQS, DynamoDB), built LocalStack-first.
Design reference: [ChaosEater](https://github.com/ntt-dkiku/chaos-eater) (Kubernetes). This repo shares no code with it; see
[docs/CHAOSEATER_MAPPING.md](docs/CHAOSEATER_MAPPING.md) for what was borrowed as an idea and what had to be redesigned.

## What exists (Major Project phase 1 = objectives 1 and 2)

| Objective | Implementation |
|---|---|
| 1. Unified adapter layer | `StorageAdapter`, `QueueAdapter`, `ComputeAdapter` (`src/serverless_chaos/adapters`). Fault injection sits in the abstract base classes, so any backend inherits it; the AWS/LocalStack backend is the first implementation. |
| 2. Closed-loop scenario generator | `generator/llm_generator.py`: prompt is conditioned on the **history** of past experiments and the **untried** fault combinations; output is schema-validated, **grounded** against the real topology and checked for **novelty**, with validate-and-retry. Baselines for evaluation: `random`, `coverage` (no-LLM closed loop), `llm-open` (LLM without history = ablation). |

Objectives 3-5 (LLM router, RAG, circuit breaker) plug into `llm/` (client protocol), `generator/llm_generator.py` (prompt context) and `experiment/runner.py` (abort hooks).

## How it works

```
 topology (discover + annotations)        history.jsonl  <------------------------+
        |                                       |                                  |
        v                                       v                                  |
   Generator (llm | coverage | random) --> Scenario (1-3 faults) --> ExperimentRunner
                                                                        |  baseline -> inject -> recovery (always rolled back)
                                       writes faults to the            v
                                       CONTROL PLANE  <--- read by --- adapters + @chaos_lambda inside your app
                                       (DynamoDB table `chaos-control`)       |
                                                                       Workload (invoke | event-flow) -> Outcome
```

Faults are injected *inside* the application through the adapters (and the `@chaos_lambda` decorator for handler-side faults).
That is the only way to affect calls that Lambdas make to each other's resources inside LocalStack, and it is why the
`runtime/` and `adapters/` packages depend only on `boto3` (they are zipped into your Lambdas).

Fault types: `LambdaLatency`, `LambdaError` (exception / timeout / throttle), `DynamoDBFault` (latency / throttle / unavailable),
`QueueFault` (delay / drop / duplicate / unavailable), `ServiceOutage`. All have hard caps on magnitude and duration; the control
plane fails open (if it is unreachable your app runs without faults).

The steady state of an event-driven system is that events *arrive*: the `eventflow` workload invokes a producer and waits for the
effect to appear in a sink table, so dropped, delayed or lost events count as failures.

## Quick start

Configuration lives in ONE file, the project-root `.env` (copy `.env.example`). The CLI, `deploy.py`, `scripts/smoke_localstack.py`
and the integration tests all find it automatically from any working directory, never override variables you already exported,
and fill in LocalStack defaults (dummy AWS credentials, region, endpoint) so you never set those by hand. `SC_REAL_AWS=1` turns the
LocalStack defaults off.

```bash
pip install -e ".[dev]"
cp .env.example .env                     # put LOCALSTACK_AUTH_TOKEN (and optional LLM settings) in it; git-ignored
docker compose up -d                     # real LocalStack
python examples/items_app/deploy.py      # idempotent: re-run any time (reuses tables, queue, functions and the SQS trigger)
python scripts/smoke_localstack.py       # 8 staged checks with hints, or: pytest -m integration -v
sls-chaos discover --annotations examples/items_app/annotations.json --out examples/items_app/topology.json

sls-chaos calibrate --config examples/items_app/experiment.json    # measure the HEALTHY system first (no faults)
sls-chaos loop --config examples/items_app/experiment.json --generator coverage --rounds 8
sls-chaos loop --config examples/items_app/experiment.json --generator llm --rounds 8   # SC_LLM_* in .env
sls-chaos report --config examples/items_app/experiment.json
```

## Two kinds of tests, never mixed

| | Unit tests | Integration tests |
|---|---|---|
| Command | `pytest` (default) | `pytest -m integration -v` |
| Backend | **moto**, an in-process AWS mock | **real LocalStack** (`docker compose up -d`) |
| Location | `tests/unit` | `tests/integration` |
| Covers | logic: adapters, fault decisions, runner, generators, deploy idempotency, env loading | what a mock cannot: Lambda executing, SQS->Lambda trigger, Lambda reaching LocalStack, faults crossing into Lambdas, rollback/recovery, a healthy baseline, a full experiment |
| If LocalStack is down | not needed | **fails loudly**; it never skips or falls back to moto |

## Unhealthy baseline: how the framework protects you and how to debug it

Before injecting anything, the runner (1) clears faults, (2) **drains the queues** (leftover dropped/delayed/duplicated/failed events
from the previous experiment would otherwise pollute the next baseline), (3) waits for the Lambdas' fault cache to expire, and only
then measures the baseline. If it is unhealthy the experiment is `aborted`: the fault was never tested, so it is **not** counted as
tried, not counted in coverage, and not shown to the LLM as a result. After `--max-aborts` (default 3) consecutive aborts the loop
stops and tells you to run `sls-chaos diagnose`.

* `sls-chaos diagnose --config ...` collects evidence: leftover faults, queue backlog, event-source-mapping state, step-by-step
  timing of single probe events, recent Lambda error logs, and prints findings.
* `sls-chaos calibrate --config ...` measures the healthy system. If success is ~100% but p95 is above the threshold, the threshold
  was unrealistic for this environment (ESM polling adds seconds); if success is low, the event flow is broken. Thresholds are never
  changed automatically.
* `p95` is the latency of requests that succeeded; failures are measured by `success_rate` (a failed event's "latency" is just the
  timeout). `p95_all_ms` keeps the raw view. Fewer than `min_samples` samples is "inconclusive", i.e. not healthy.
* The event-sink poller is built without fault injection, so a DynamoDB fault cannot break the measuring instrument itself.

## Evaluation plan for objective 2

Run `random`, `coverage`, `llm-open`, `llm` for the same number of rounds on the same app (`--history` per run) and compare from the
history files: fault-combination coverage, number of distinct steady-state violations found, and rounds until the first violation.

## Status / honesty notes

* Unit tests (54): pass against moto. Integration tests (10) are written but **have not been run by the author**: the build sandbox
  could not pull the LocalStack image (registry blocked) and had no LocalStack token. Run `pytest -m integration -v` and
  `sls-chaos diagnose` on your machine; they are the source of truth for the baseline problem.
* Lambda-to-LocalStack networking (`SC_LAMBDA_ENDPOINT_URL`, default `http://localhost.localstack.cloud:4566`) depends on your Docker setup.
* Handler-side Lambda faults require `@chaos_lambda`; calls made with raw boto3 (not the adapters) are not affected.
* Only SQS is covered on the messaging side (EventBridge/SNS/streams are natural next adapters).
