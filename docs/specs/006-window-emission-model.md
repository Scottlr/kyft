# 006 - Window Emission Model

Define the public emission contracts for opened and closed windows.

## Scope

- Add an enum or equivalent discriminator for open and close.
- Add a generic window emission type containing window name, key, event, and transition kind.
- Include a sequence number or processing index only if needed by ingestion tests.
- Keep interval timestamps out until event-time support is deliberately introduced.

## Acceptance

- Public emissions can represent open and close transitions.
- Tests verify the model is immutable enough for consumers.

## Out Of Scope

- Duration calculation.
- Overlap or residual analysis.

