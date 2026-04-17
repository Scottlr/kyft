# Kyft Staged Implementation Specs

This directory contains the staged implementation plan for `Kyft`.

Each numbered spec is intended to be implemented as a small, coherent commit.
Specs are ordered so each stage leaves the codebase valid and builds naturally on
earlier work. Implementation should proceed one spec at a time without batching
later behavior into earlier commits.

The first pass intentionally favors a compact public model:

- define windows
- define active state from events
- roll child windows up to parent windows
- feed events
- receive open and close emissions

Advanced interval overlap and residual analysis are planned as later query/runtime
concerns rather than exposed in the initial builder surface.

