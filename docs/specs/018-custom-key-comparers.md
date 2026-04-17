# 018 - Custom Key Comparers

Allow custom equality where key types need non-default comparison.

## Scope

- Add comparer support at window definition time only if the existing key model needs it.
- Apply comparers consistently to runtime state dictionaries.
- Keep the common API overload simple.

## Acceptance

- Tests cover a case-insensitive string key.
- Default comparer behavior remains unchanged.

## Out Of Scope

- Global comparer registries.
- Serialization of comparer configuration.

