<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/brand/spanfold-logo-readme-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/assets/brand/spanfold-logo-readme-light.svg">
    <img src="docs/assets/brand/spanfold-logo-readme-light.svg" alt="Spanfold" width="280">
  </picture>
</p>

# Spanfold

**Track when a predicate held. Compare those periods across sources.**

When you have multiple systems, providers, or pipeline stages reporting observations, Spanfold answers the questions a latest-value store cannot:

- **When** did each source think this condition was true — and for how long?
- **Where** did two sources agree, diverge, or run out of sync?
- **Who** saw a period that another missed, reported late, or recovered from early?
- **What** was knowable at a specific point in time, without leaking future data?

---

## How It Works

Spanfold watches an event stream and converts predicate changes into recorded windows. A predicate can be anything — a boolean flag, a value crossing a threshold, a status code changing, or any other condition you define. When the predicate transitions, a window opens. When it transitions back, the window closes.

```
              open                                close
               ↓                                   ↓
Events:  ──────●───────────────────────────────────●──────────
Window:        [══════════════  3m 40s  ═════════════]
```

Windows accumulate per source and key. The useful analysis begins when you compare them.

```
time  ───────────────────────────────────────────────────────►

A     ──[════════════════════]───────────────────────────────
B     ─────────[═══════════════════════]──────────────────────

         [ ─A─][──────── overlap ──────][──────── B ────────]
```

Spanfold emits this as structured temporal evidence — overlap duration, residual, missing periods, gaps, lead/lag timing, coverage percentages — not a single aggregated number.

---

## Predicates Are Not Limited To Booleans

A window opens when a predicate **changes**. The predicate itself can evaluate anything:

| Predicate type     | Example                                            |
|--------------------|----------------------------------------------------|
| Boolean            | `isUp == true`                                     |
| Threshold          | `cpuPercent > 80`                                  |
| Enum / status      | `status == "degraded"`                             |
| Numeric range      | `latencyMs >= 500 && latencyMs < 2000`             |
| Any value change   | alert level changed, model output changed, annotation updated |

Spanfold does not care about the raw value — it records the period where your predicate held, and closes it when the predicate no longer holds.

---

## Core Flow

```
┌──────────────┐             ┌──────────────┐             ┌──────────────┐
│   Events /   │  predicate  │   Recorded   │  comparison │   Temporal   │
│ Observations │────────────►│   Windows    │────────────►│   Evidence   │
└──────────────┘ transitions └──────────────┘    plan     └──────────────┘
                                                                  │
                                           ┌──────────────────────┼─────────────────────┐
                                           ▼                      ▼                     ▼
                                    Overlap · Residual      Gap · Coverage       Lead/Lag · As-of
                                    Missing · Symmetric     Diagnostics           Explanations
```

1. Define when a keyed condition holds.
2. Ingest events or observations.
3. Record windows as the predicate opens and closes.
4. Run a comparison plan across sources, lanes, or stages.
5. Export rows, summaries, diagnostics, and explanations.

---

## What Category Is This?

Spanfold sits between event processing and analytics. It is not a general stream processor, metrics library, database abstraction, or dashboard.

It records the periods where an interpreted condition held, then compares those periods as first-class temporal data. Narrower than a stream processor, more structured than hand-written interval joins.

---

## Concrete Use Cases

- **Multi-provider outage audit**: Compare monitoring providers and find where one reported a degraded period that another missed, reported late, or recovered from sooner.
- **Backtesting and replays**: Filter to windows that were knowable at a decision point — no future leakage, even for still-open windows.
- **Pipeline stage divergence**: Compare stages in a processing pipeline to see where state diverged, lagged, disappeared, or gained extra coverage.
- **Detector evaluation**: Audit a classifier, detector, or rules engine against a ground-truth source using only data that was available at inference time.
- **Consensus and quorum**: Analyze multi-source agreement, quorum behavior, lane silence, or source matrix coverage without hand-written joins.

---

## Core Concepts

### Windows

A window is a half-open period where a predicate held for a key. Windows can be closed, still open, or clipped to an explicit live horizon.

### Sources And Lanes

Sources identify where an observation came from: providers, monitors, gateways, pipeline stages, model versions, or any other lane that can report observations.

### Segments And Tags

Segments split active windows when important analytical context changes. Tags attach descriptive metadata without splitting the window.

### Comparisons

A comparison chooses a target side, a comparison side, a scope, normalization rules, and one or more comparator families. The result is structured temporal evidence rather than a single pass/fail value.

### Known-At Safety

Known-at filtering separates when a state happened from when it was available to the system. That matters for backtests, audits, replays, and decision-point analysis.

### Live Horizons

Historical comparisons normally expect closed windows. Live comparisons use an explicit horizon to evaluate still-open windows and preserve provisional row metadata.

---

## Comparator Families

| Comparator         | What it measures                                    |
|--------------------|-----------------------------------------------------|
| Overlap            | Duration where both sides agreed                    |
| Residual           | Target-only duration (what the comparison side missed) |
| Missing            | Comparison-only duration (what the target missed)   |
| Coverage           | Magnitude and coverage percentage                   |
| Gap                | Empty spaces inside an observed scope               |
| Symmetric diff     | Disagreement in both directions                     |
| Containment        | Whether one period stays inside another             |
| Lead / Lag         | Transition timing drift between sources             |
| As-of              | Point-in-time lookup without future leakage         |

Full comparator documentation:

- [Reference package README](packages/dotnet/README.md)
- [Comparator reference](docs/comparator-reference.md)
- [Comparison guide](docs/comparison-guide.md)

---

## Why Not Just X?

### Latest-State Tracking

Latest-state tables answer what is true now. Spanfold answers when it was true, who saw it, and whether another lane missed it.

### Event Sourcing

Event sourcing stores durable facts and rebuilds state. Spanfold analyzes the periods where that state held after those facts have been interpreted.

### Stream Processors

Stream processors handle online computation, routing, enrichment, and continuous aggregation. Spanfold stays narrower: record interpreted state windows, compare their temporal evidence.

### Observability And Metrics Tools

Metrics tools aggregate time into counters, histograms, and dashboards. Spanfold keeps individual windows and emits comparison rows with full temporal structure.

### Storing Windows Directly In A Database

A database can persist windows, but it will not provide staged comparison plans, source selectors, normalization, live finality, known-at filtering, diagnostics, or deterministic exports.

---

## Repository Layout

```text
packages/
  dotnet/
    src/
    tests/
    samples/
    benchmarks/
    docs/
    Spanfold.slnx
  python/
    src/
    tests/
    samples/
    docs/
    pyproject.toml
docs/
  index.html
  assets/
```

## Language Packages

| Package | Status | Notes |
|---|---|---|
| [.NET / C#](packages/dotnet/README.md) | Reference implementation | Complete current API surface, CLI, testing helpers, samples, benchmarks, and package docs. |
| [Python](packages/python/README.md) | Partial typed port | Early package port following the same behavioral model; not yet the canonical API surface. |

Future language ports should live under `packages/<language>` with their own package metadata, tests, examples, and package-level README.

## Website

The static documentation site lives in [docs](docs/index.html). GitHub Pages deploys that folder through the repository Pages setting: deploy from branch, folder `/docs`.

Package-local documentation remains with each implementation under `packages/<language>/docs`, while `docs/` is the root-level deploy target for the public site.

## Working With Packages

Run the reference package tests:

```bash
dotnet test packages/dotnet/Spanfold.slnx
```

Run the Python port tests:

```bash
cd packages/python
python -m pip install -e ".[dev]"
pytest
```

## Documentation

- [Public site](docs/index.html)
- [Get started](docs/get-started.html)
- [Use cases](docs/use-cases.html)
- [Visualiser](docs/visualiser.html)
- [API reference](docs/api.html)
- [.NET package README](packages/dotnet/README.md)
- [Python package README](packages/python/README.md)
