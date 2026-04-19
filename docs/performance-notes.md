# Performance Notes

## 038 Alignment Allocation Pass

The first benchmark-backed optimization target is comparison alignment. The
original alignment path used LINQ grouping, ordering, and per-group array
materialization before segment construction. The current path builds one
sortable array of normalized windows, sorts it deterministically, and processes
contiguous scope groups in place.

The benchmark suite added in spec 037 covers `Prepare`, `Align`, overlap,
residual, coverage, and multi-comparator runs across small, medium,
high-overlap, high-cardinality, and many-source scenarios. Future tuning should
use those benchmarks before adding pooling or lower-level allocation strategies.
