# 003 - Public Entry Point

Introduce the compact public entry point for configuring event pipelines.

## Scope

- Add a static `Kyft` type.
- Add `Kyft.For<TEvent>()`.
- Return a builder type that can be extended by later specs.
- Keep the builder intentionally minimal.

## Acceptance

- A test can call `Kyft.For<SampleEvent>()`.
- No runtime pipeline is built yet.

## Out Of Scope

- Window definitions.
- Roll-up definitions.
- Event ingestion.

