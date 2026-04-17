# 013 - Roll-Up Key Validation

Tighten key and name behavior around roll-ups.

## Scope

- Decide and enforce whether null keys are allowed.
- Ensure roll-up keys are stable dictionary keys.
- Add useful errors for duplicate window names.
- Add tests for invalid configuration and invalid event-derived keys.

## Acceptance

- Invalid names and null keys fail predictably.
- Existing happy path behavior is unchanged.

## Out Of Scope

- Custom key comparers.
- Complex key normalization.

