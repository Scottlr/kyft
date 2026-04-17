# 005 - Build Empty Pipeline

Add `Build()` and a minimal pipeline object.

## Scope

- Add `Build()` to the builder.
- Return a pipeline object for the configured event type.
- Preserve configured descriptors internally.
- Keep the pipeline inert until ingestion is added.

## Acceptance

- A configured builder can be built.
- Tests verify descriptors survive build through observable metadata or controlled internal access.

## Out Of Scope

- Event ingestion.
- Window open or close emissions.

