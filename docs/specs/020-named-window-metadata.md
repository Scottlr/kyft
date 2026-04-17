# 020 - Named Window Metadata

Expose compact metadata about configured windows.

## Scope

- Add read-only metadata for window names, hierarchy, and event type.
- Avoid exposing runtime dictionaries or mutable descriptors.
- Use metadata in diagnostics-oriented tests.

## Acceptance

- Consumers can inspect configured window names and parent-child relationships.
- Metadata does not allow runtime mutation.

## Out Of Scope

- Full descriptor serialization.
- Visualization helpers.

