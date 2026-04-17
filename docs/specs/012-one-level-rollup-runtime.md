# 012 - One-Level Roll-Up Runtime

Evaluate one parent roll-up from one child window.

## Scope

- Maintain child active state grouped by parent key.
- Recompute the parent predicate when a child changes state.
- Emit parent open and close transitions.
- Emit child transition before parent transition for the same event.

## Acceptance

- Tests cover all children active opening a market window.
- Tests cover one child becoming inactive closing the market window.
- Parent state is partitioned by parent key.

## Out Of Scope

- Multiple roll-ups from one child.
- Multi-level roll-up chains.

