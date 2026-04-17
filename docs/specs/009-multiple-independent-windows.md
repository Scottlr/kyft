# 009 - Multiple Independent Windows

Allow more than one independent source window to be configured.

## Scope

- Permit repeated `.Window(...)` calls where each window tracks its own state.
- Process all configured source windows for each event.
- Emit transitions in definition order.
- Validate duplicate names if name uniqueness is required by later lookup behavior.

## Acceptance

- Tests cover two windows on the same event type with distinct keys or predicates.
- Emission ordering is deterministic.

## Out Of Scope

- Parent roll-ups.
- Cross-window dependencies.

