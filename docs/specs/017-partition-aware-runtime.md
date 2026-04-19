# 017 - Partition-Aware Runtime

Allow the same logical key to be tracked independently by partition.

## Scope

- Add optional partition identity to ingestion.
- Include partition identity in runtime state keys.
- Preserve partition identity in emissions.
- Keep no-partition behavior as the default.

## Acceptance

- Tests cover the same device key in two partitions opening independently.
- Closing one partition does not close the other.

## Out Of Scope

- Partition-aware roll-up merging.
- Distributed execution.
