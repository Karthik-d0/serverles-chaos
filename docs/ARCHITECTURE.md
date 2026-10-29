# Architecture and design decisions

**Why in-app adapters instead of an external fault proxy?** Inside LocalStack, Lambdas call DynamoDB/SQS themselves; nothing the
experiment runner does on the outside touches those calls. Putting the injection point in a thin adapter library that the app uses
(plus one decorator per handler) makes every call observable and faultable, keeps faults deterministic and per-resource, and works
unchanged on real AWS. The cost is that the app must use the adapters. A network-level fault proxy is a possible later backend.

**Control plane.** Active faults are items in a DynamoDB table (`chaos-control`) with an expiry time, read with a short cache by every
adapter. A forgotten fault therefore expires on its own, and runner crashes cannot leave a system broken. Reads fail open. Each process caches the fault list for 0.5 s, so a fault (or its rollback) reaches deployed Lambdas up to ~0.5 s after the runner writes it; the runner's own phases are seconds long, so this is negligible, but keep fault windows well above 1 s.

**Where each fault is applied** (to avoid double counting): `LambdaLatency`, `LambdaError(exception|timeout)` are applied in the handler
(`@chaos_lambda`), so they also hit SQS-triggered invocations. `LambdaError(throttle)` and `ServiceOutage(lambda)` also apply on invoke.
Storage faults apply per call (`read`/`write`). Queue `drop`/`duplicate`/`delay` apply on `send`; `unavailable` on send and receive.

**Dependency boundary.** `runtime/` and `adapters/` import only boto3 + stdlib (enforced by a test) because they ship in Lambda zips.
Everything else (pydantic models, generators, runner, CLI) runs on the operator's machine.

**Closed loop.** `History` stores every scenario and outcome. The generator sees the last experiments, which combinations violated the
steady state, and which (type, target, variant) combinations are still untried; it must add at least one untried combination. Coverage
= tried / all combinations derivable from the topology.

**Extension points for objectives 3-5.** `LLMClient` protocol (router), `LLMGenerator._user` (RAG context), `ExperimentRunner` fault
window loop (circuit breaker on rolling p95 / error rate).
