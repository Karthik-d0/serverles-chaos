# Relation to ChaosEater

ChaosEater (Kikuta et al., ASE 2025) automates the chaos-engineering cycle for Kubernetes with an LLM agent workflow.
This project reuses its *ideas* and none of its code.

| ChaosEater idea | Here |
|---|---|
| Cycle: hypothesis -> experiment -> analysis -> improvement | Steady state + scenario generation -> runner (baseline/inject/recovery) -> outcome -> history. Analysis/improvement agents are future work. |
| Define the steady state first, validate it before injecting | `SteadyState` + baseline phase; an unhealthy baseline aborts the experiment so a fault is never blamed for existing damage. |
| Pre-validation / fault-injection / post-validation phases | `ExperimentRunner`: baseline, fault window, recovery (`recovered` flag). |
| Schema-constrained LLM output, validate-and-retry with the error fed back | `LLMGenerator`: pydantic schema + grounding + novelty checks, errors returned to the model, `max_attempts`. |
| Chaos tool abstraction (`CEToolBase`) | Replaced by the three adapters + a runtime control plane. |
| Chaos Mesh faults selected by pod label | Serverless faults selected by resource name (function / table / queue / service). |
| Chaos Mesh Workflow YAML applied with kubectl | No workflow engine: faults are written to a control table and applied in-process by adapters. |
| K8s API / k6 steady-state checks | Workloads (`invoke`, `eventflow`) that measure success rate, latency and end-to-end event delivery. |
| One-shot scenario per cycle | **New:** history-conditioned, coverage-aware generation (objective 2). |

What does not transfer from Kubernetes: ephemeral functions have no pods to kill or sidecars to inject, failure propagates through
events rather than service calls, and the "system under test" is a graph of managed services reached through SDK calls.
