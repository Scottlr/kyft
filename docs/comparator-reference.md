# Comparator Reference

Comparators operate over aligned half-open segments.

## Overlap

`Overlap()` emits rows where target and comparison windows are active at the
same time.

Each `OverlapRow` contains:

- `WindowName`
- `Key`
- `Partition`
- `Range`
- `TargetRecordIds`
- `AgainstRecordIds`

Use overlap rows when the question is "where did both sources agree that the
state was active?"

## Residual

`Residual()` emits rows where the target is active without comparison coverage.

Each `ResidualRow` contains:

- `WindowName`
- `Key`
- `Partition`
- `Range`
- `TargetRecordIds`

Use residual rows when the question is "what did the target see that the
comparison source missed?"

## Missing

`Missing()` emits rows where comparison windows are active without target
coverage.

Each `MissingRow` contains:

- `WindowName`
- `Key`
- `Partition`
- `Range`
- `AgainstRecordIds`

Use missing rows when the question is "what did the comparison source see that
the target did not?"

## Coverage

`Coverage()` emits segment rows and aggregate summaries.

Each `CoverageRow` contains:

- `WindowName`
- `Key`
- `Partition`
- `Range`
- `TargetMagnitude`
- `CoveredMagnitude`
- `TargetRecordIds`
- `AgainstRecordIds`

For processing-position ranges, magnitude is position length. For timestamp
ranges, magnitude is duration ticks. Coverage summaries roll those rows up by
window name, key, and partition.

Use coverage when the question is "how much of the target did the comparison
source cover?"

## Gap

`Gap()` emits rows for uncovered spaces inside an observed scope envelope.

Each `GapRow` contains:

- `WindowName`
- `Key`
- `Partition`
- `Range`

Gap is not the same as residual. Residual means the target was active without
comparison coverage. Gap means neither side was active between observed
segments. Kyft does not invent boundary gaps before the first observed segment
or after the last observed segment unless a later query-scope policy makes those
boundaries explicit.

## Symmetric Difference

`SymmetricDifference()` emits target-only and comparison-only disagreement rows
in one result set.

Each `SymmetricDifferenceRow` contains:

- `WindowName`
- `Key`
- `Partition`
- `Range`
- `Side`
- `TargetRecordIds`
- `AgainstRecordIds`

Use symmetric difference when the question is "where did these histories
disagree at all?" It is equivalent to looking at target residuals and
comparison-only missing segments together.

## Containment

`Containment()` checks whether target windows are covered by comparison windows.
The direction is intentional: target rows are the windows being validated, and
comparison rows are the expected containers.

Each `ContainmentRow` contains:

- `WindowName`
- `Key`
- `Partition`
- `Range`
- `Status`
- `TargetRecordIds`
- `ContainerRecordIds`

Statuses are:

- `Contained`: the target segment is covered by at least one comparison window
- `LeftOverhang`: the target starts before container coverage
- `RightOverhang`: the target continues after container coverage
- `NotContained`: the target segment is uncovered but is not at either edge

Use containment for invariants such as "task running only while host online" or
"child state only while parent state is active."

## Lead/Lag

`LeadLag(transition, axis, toleranceMagnitude)` measures the nearest comparison
transition for each target transition in the same window name, key, and
partition.

Each `LeadLagRow` contains:

- `WindowName`
- `Key`
- `Partition`
- `Transition`
- `Axis`
- `TargetPoint`
- `ComparisonPoint`
- `DeltaMagnitude`
- `ToleranceMagnitude`
- `IsWithinTolerance`
- `Direction`
- `TargetRecordId`
- `ComparisonRecordId`

Negative deltas mean the target led. Positive deltas mean the target lagged.
For processing-position comparisons, tolerance is measured in positions. For
timestamp comparisons, tolerance is measured in ticks.

Lead/lag is transition-based and does not prove causality. Use it to quantify
provider latency, sequencing drift, or replay skew.

## As-Of

`AsOf(direction, axis, toleranceMagnitude)` performs point-in-time lookup from
each target start point to comparison start points in the same window name, key,
and partition.

Each `AsOfRow` contains:

- `WindowName`
- `Key`
- `Partition`
- `Axis`
- `Direction`
- `TargetPoint`
- `MatchedPoint`
- `DistanceMagnitude`
- `ToleranceMagnitude`
- `Status`
- `TargetRecordId`
- `MatchedRecordId`

Use `AsOfDirection.Previous` for point-in-time enrichment that must not leak
future comparison records into the target. `Next` explicitly allows future
matches. `Nearest` can match on either side and reports ambiguous equal-distance
matches with a diagnostic while choosing deterministically.
