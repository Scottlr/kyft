# 014 - Callback Subscription API

Add optional callbacks for consumers that prefer push-style emissions.

## Scope

- Add a way to register an emission callback during configuration or pipeline construction.
- Invoke callbacks when ingestion produces emissions.
- Preserve return-value emissions from ingestion.
- Keep callback ordering deterministic.

## Acceptance

- Tests cover callback invocation for open and close.
- Tests verify returned emissions and callback emissions match.

## Out Of Scope

- Async callbacks.
- Error handling policies beyond normal exception propagation.

