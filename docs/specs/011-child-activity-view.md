# 011 - Child Activity View

Implement the public child activity view used by roll-up predicates.

## Scope

- Add a compact read-only view over child states for one parent key.
- Provide `AllActive()`, `AnyActive()`, `ActiveCount`, and `TotalCount`.
- Define behavior for no children explicitly.

## Acceptance

- Unit tests cover all helper methods.
- No runtime roll-up state uses the view yet except construction tests.

## Out Of Scope

- Parent window opening and closing.
- Removing stale children.

