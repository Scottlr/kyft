# 010 - Basic Roll-Up Definition API

Add the public `.RollUp(...)` definition API after a child window.

## Scope

- Add `.RollUp(name, key, isActive)` to the builder returned from `.Window(...)`.
- Model the roll-up as a parent descriptor attached to the previous child window.
- Add a child activity view passed to the roll-up predicate.
- Keep the API close to `children => children.AllActive()`.

## Acceptance

- The example API shape compiles through `.Window(...).RollUp(...).Build()`.
- Roll-up descriptors are captured.
- No roll-up runtime behavior is required yet.

## Out Of Scope

- Evaluating child activity.
- Multi-level roll-ups.

