# 015 - Batch Ingestion

Add a small batch ingestion convenience API.

## Scope

- Add a method that accepts an enumerable of events.
- Process events in source order.
- Return a flattened sequence or batch result containing emissions.
- Avoid hidden parallelism.

## Acceptance

- Tests verify order across multiple events and keys.
- Single-event ingestion remains the primary primitive.

## Out Of Scope

- Async streams.
- Backpressure.

