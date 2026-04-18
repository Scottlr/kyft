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
