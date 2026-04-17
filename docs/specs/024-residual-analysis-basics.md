# 024 - Residual Analysis Basics

Add a minimal residual analysis helper for source comparisons.

## Scope

- Given intervals for a target source, subtract common overlap from comparison sources.
- Return the unique residual segments for the target source.
- Keep the API separate from the main builder surface.

## Acceptance

- Tests cover full overlap, partial overlap, and no overlap.
- Residual output preserves target source identity.

## Out Of Scope

- Multi-dimensional attribution.
- Complex visualization output.

