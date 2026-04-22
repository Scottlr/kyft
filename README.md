<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="pages/assets/brand/spanfold-logo-readme-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="pages/assets/brand/spanfold-logo-readme-light.svg">
    <img src="pages/assets/brand/spanfold-logo-readme-light.svg" alt="Spanfold" width="280">
  </picture>
</p>

# Spanfold

Spanfold records temporal state windows and compares them across sources,
providers, lanes, or pipeline stages.

It is for systems where the important question is not only "what is the latest
value?", but:

- when was this state active?
- who observed it?
- who missed it?
- where did sources overlap, diverge, lag, or leave gaps?
- what was knowable at a specific point in time?

Spanfold turns event predicates into recorded windows, then gives applications a
staged model for auditing those windows.

## What Category Is This?

Spanfold sits between event processing and analytics.

It is not a general stream processor, metrics library, database abstraction, or
dashboard. It records the periods where interpreted state was active, then
compares those periods as first-class temporal data.

The core flow is:

1. Define when a keyed state is active.
2. Ingest events or state observations.
3. Record open and closed windows.
4. Compare windows across sources, lanes, or stages.
5. Export rows, summaries, diagnostics, and explanations.

## Repository Layout

This repository is organized as a multi-language project:

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
pages/
  index.html
  assets/
docs -> pages
```

## Language Packages

| Package | Status | Notes |
| --- | --- | --- |
| [.NET / C#](packages/dotnet/README.md) | Reference implementation | Complete current API surface, CLI, testing helpers, samples, benchmarks, and package docs. |
| [Python](packages/python/README.md) | Partial typed port | Early package port following the same behavioral model; not yet the canonical API surface. |

Future language ports should live under `packages/<language>` with their own
package metadata, tests, examples, and package-level README.

## Website

The static documentation site lives in [pages](pages/index.html). GitHub Pages
deploys that folder through [.github/workflows/pages.yml](.github/workflows/pages.yml).
The root `docs` path is a compatibility symlink to `pages` for repositories that
still have Pages configured to deploy from `/docs`.

Package-local documentation remains with each implementation under
`packages/<language>/docs`, while `pages/` is the root-level deploy target for
the public site.

## Concrete Use Cases

- Compare monitoring providers and find where one reported an outage that
  another missed, reported late, or recovered from sooner.
- Audit a detector, model, or rules engine using only the windows that were
  knowable at the decision point.
- Run live analysis over still-open windows and separate final rows from
  provisional rows.
- Compare stages in a processing pipeline to see where state diverged, lagged,
  disappeared, or gained extra coverage.
- Analyze multi-source consensus, quorum behavior, lane silence, or source
  matrix coverage without hand-written interval joins.

## Core Concepts

### Windows

A window is a half-open period where a state predicate stayed true for a key.
Windows can be closed, still open, or clipped to an explicit live horizon.

### Sources And Lanes

Sources identify where an observation came from: providers, monitors, gateways,
pipeline stages, model versions, or any other lane that can report state.

### Segments And Tags

Segments split active windows when important analytical context changes. Tags
attach descriptive metadata without splitting the window.

### Comparisons

A comparison chooses a target side, a comparison side, a scope, normalization
rules, and one or more comparator families. The result is structured temporal
evidence rather than a single pass/fail value.

### Known-At Safety

Known-at filtering separates when a state happened from when it was available to
the system. That matters for backtests, audits, replays, and decision-point
analysis.

### Live Horizons

Historical comparisons normally expect closed windows. Live comparisons use an
explicit horizon to evaluate still-open windows and preserve provisional row
metadata.

## Comparator Families

- Overlap shows agreement.
- Residual shows target-only duration.
- Missing shows comparison-only duration.
- Coverage summarizes magnitude and coverage.
- Gap shows empty spaces inside an observed scope.
- Symmetric difference shows disagreement in both directions.
- Containment checks whether one state stays inside another state.
- Lead/lag measures transition timing drift.
- As-of performs point-in-time lookup without future leakage.

The full comparator surface is currently documented in the reference package and
the public site:

- [Reference package README](packages/dotnet/README.md)
- [Comparator reference](pages/comparator-reference.md)
- [Comparison guide](pages/comparison-guide.md)

## Why Not Just X?

### Latest-State Tracking

Latest-state tables answer what is true now. Spanfold answers when it was true,
who saw it, and whether another lane missed it.

### Event Sourcing

Event sourcing is good for storing durable facts and rebuilding state. Spanfold
analyzes the periods where state was active after those facts have been
interpreted.

### Stream Processors

Stream processors are good for online computation, routing, enrichment, and
continuous aggregation. Spanfold stays narrower: it records interpreted state
windows and compares their temporal evidence.

### Observability And Metrics Tools

Metrics tools are good for dashboards, counters, histograms, and alerts. They
usually compress time into aggregates. Spanfold keeps individual windows and
emits comparison rows.

### Storing Windows Directly In A Database

A database can persist windows, but it will not provide staged comparison plans,
source selectors, normalization, live finality, known-at filtering, diagnostics,
or deterministic exports by itself.

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

- [Public site](pages/index.html)
- [Get started](pages/get-started.html)
- [Use cases](pages/use-cases.html)
- [Visualiser](pages/visualiser.html)
- [API reference](pages/api.html)
- [.NET package README](packages/dotnet/README.md)
- [Python package README](packages/python/README.md)
