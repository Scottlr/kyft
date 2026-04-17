# 021 - Interval Recording

Record closed intervals as runtime history for later analysis.

## Scope

- Add an optional interval history mode.
- Record start and end processing positions for closed windows.
- Keep open windows distinguishable from closed history.
- Include source and partition identities where available.

## Acceptance

- Tests verify one closed interval is recorded after open then close.
- Open windows are not reported as closed intervals.

## Out Of Scope

- Event-time timestamps.
- Overlap analysis.
- Residual subtraction.

