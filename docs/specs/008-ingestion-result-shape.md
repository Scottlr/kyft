# 008 - Ingestion Result Shape

Make event ingestion return a stable result object.

## Scope

- Return a result containing zero or more emissions for one input event.
- Avoid exposing mutable runtime collections.
- Keep the API convenient for simple event loops.

## Acceptance

- Tests verify empty and non-empty emission results.
- Existing single-window runtime behavior still passes.

## Out Of Scope

- Batch ingestion.
- Async ingestion.

