# 016 - Source-Aware Events

Introduce optional source identity for analysis without forcing it into every event type.

## Scope

- Add an ingestion overload that accepts a source identifier alongside the event.
- Preserve source identity in emissions.
- Keep existing ingestion overloads working with no source.

## Acceptance

- Tests verify source identity appears on emissions.
- Tests verify no-source ingestion remains compact.

## Out Of Scope

- Source-level partition isolation.
- Residual analysis.

