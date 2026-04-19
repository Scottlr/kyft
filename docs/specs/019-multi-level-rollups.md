# 019 - Multi-Level Roll-Ups

Extend roll-ups so a parent can itself feed another parent.

## Scope

- Allow `.RollUp(...).RollUp(...)` chains.
- Treat parent activity transitions as child activity for the next level.
- Preserve deterministic child-to-parent emission order.

## Acceptance

- Tests cover device to region to cluster roll-up.
- Closing behavior propagates upward.

## Out Of Scope

- Roll-up DAGs with shared parents.
- Cycles or complex graph validation beyond simple chains.
