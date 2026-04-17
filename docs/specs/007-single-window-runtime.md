# 007 - Single Window Runtime

Implement runtime tracking for one configured window.

## Scope

- Add event ingestion to the built pipeline.
- Track active/inactive state per selected key.
- Emit open when a key transitions from inactive to active.
- Emit close when a key transitions from active to inactive.
- Emit nothing when state does not change.

## Acceptance

- Tests cover open, unchanged active, close, and unchanged inactive.
- State is partitioned by key.

## Out Of Scope

- Multiple window definitions.
- Roll-ups.
- Callbacks.

