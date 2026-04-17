# 023 - Overlap Query Basics

Add the first query helper for overlapping closed intervals.

## Scope

- Add a query API over recorded intervals.
- Return pairs or groups of intervals that overlap for the same logical window scope.
- Keep the algorithm simple and testable.

## Acceptance

- Tests cover overlapping and non-overlapping intervals.
- Query behavior is source-aware when source identities are present.

## Out Of Scope

- Residual subtraction.
- Optimized interval trees.

