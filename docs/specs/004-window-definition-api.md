# 004 - Window Definition API

Add the public API for defining a state-driven event window.

## Scope

- Add `.Window(name, key, isActive)` to the event pipeline builder.
- Store a descriptor containing the window name, key selector, and active-state selector.
- Keep selector types strongly typed to the source event.
- Validate required arguments.

## Acceptance

- A test can define `SelectionSuspension` using an event key and `isActive`.
- Null argument validation is covered.
- No events are processed yet.

## Out Of Scope

- Multiple windows.
- Roll-ups.
- Build output behavior.

