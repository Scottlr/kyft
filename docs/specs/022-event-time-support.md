# 022 - Event-Time Support

Allow consumers to provide event timestamps for intervals.

## Scope

- Add optional event-time selector configuration.
- Include start and end timestamps on interval records when configured.
- Preserve processing-position intervals when event time is absent.

## Acceptance

- Tests verify interval timestamps derive from opening and closing events.
- Existing processing-position behavior remains valid without timestamps.

## Out Of Scope

- Watermarks.
- Out-of-order correction.

