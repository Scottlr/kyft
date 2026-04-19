# API Shape Review

This review captures the current public shape before adding another feature
batch. The goal is to keep Kyft readable for humans and predictable for agents.

## Project Layout

- `src/Kyft` is the primary package.
- `src/Kyft.Testing` is a packable helper package for fixtures, snapshots,
  virtual horizons, and assertions.
- `tests/Kyft.Tests` is the only xUnit test project.
- `src/Kyft.Cli` is a non-packable fixture runner prototype.
- `benchmarks/Kyft.Benchmarks` is a non-packable BenchmarkDotNet project.

`Kyft.Testing` can look like a test project by name, but it is intentionally
under `src` because consumers should be able to reference it from their own test
suites.

## Core Concepts

The current naming is coherent enough to keep:

- `Window` is the domain state span being tracked.
- `WindowRecord` is a recorded open or closed span.
- `WindowIntervalHistory` is the append-oriented history and query surface.
- `ComparisonPlan` is the inspectable question.
- `PreparedComparison` is selection plus normalization.
- `AlignedComparison` is deterministic segmentation.
- `ComparisonResult` is materialized rows, summaries, diagnostics, and finality.

The comparison builder flow remains the right shape:

```csharp
history.Compare("Provider QA") // Start a staged comparison over recorded windows.
    .Target("provider-a", selector => selector.Source("provider-a")) // Select the baseline source.
    .Against("provider-b", selector => selector.Source("provider-b")) // Select the comparison source.
    .Within(scope => scope.Window("DeviceOffline")) // Limit the question to one window family.
    .Normalize(normalization => normalization.KnownAtPosition(42)) // Restrict records to what was known at position 42.
    .Using(comparators => comparators.Overlap().Coverage()) // Emit agreement and coverage metrics.
    .Run(); // Execute the comparison.
```

It reads in the order an analyst thinks: baseline, comparison source, scope,
normalization, metric.

## Modern C# Usage

Records are a good fit for immutable data contracts such as rows, summaries,
plans, diagnostics, and metadata.

Primary constructors are a good fit for small immutable result or exception
types. They are not a good default for mutable builders or runtime state owners,
where explicit fields and constructor bodies communicate ownership better.

Deconstruction should be offered where it improves examples without hiding
meaning:

- `IngestionResult<TEvent>` deconstructs to `(emissions, hasEmissions)`.
- `ChildActivityView` deconstructs to `(activeCount, totalCount)`.

## Freeze Risks

The main names to revisit before v1 are:

- `WindowIntervalHistory`: accurate but slightly long. It is worth keeping until
  a shorter name proves clearer.
- `ComparisonNormalizationPolicy`: accurate and stable. Good for public
  contracts.
- `Against`: readable in the fluent flow. Keep.
- `RunLive`: clear for ongoing open windows. Keep, and document finality.

## Next API Work

- Keep fixture execution truthful: fixture JSON should not silently drop sources,
  comparators, open windows, or live horizons.
- Add small result query helpers rather than making users inspect every row
  collection manually.
- Keep source matrix helpers directional and explicit.
- Avoid hidden global registries in core hot paths. Expose comparator catalogs
  for tooling, but keep execution deterministic from the plan declarations.
