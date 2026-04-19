# v1 API Freeze Readiness

Kyft is not marked v1 yet, but the public comparison surface now has the checks
needed before a freeze decision.

## Freeze Gates

- `Kyft` and `Kyft.Testing` generate XML documentation and treat missing public
  XML comments as build errors.
- `Kyft` and `Kyft.Testing` are the packable projects. CLI and benchmarks stay
  non-packable.
- Contract fixtures cover language-neutral comparison examples.
- Benchmark smoke tests compile and execute the baseline benchmark paths.
- Runtime plan criticism flags common unsafe analytical plans before users rely
  on output.

## Baseline Commands

```bash
dotnet test
dotnet build benchmarks/Kyft.Benchmarks/Kyft.Benchmarks.csproj
dotnet pack src/Kyft/Kyft.csproj
dotnet pack src/Kyft.Testing/Kyft.Testing.csproj
```

## Complexity Notes

- Preparation sorts recorded windows, evaluates selectors, and normalizes ranges:
  approximately `O(w log w + w * s)` for windows `w` and selectors `s`.
- Alignment sorts normalized windows once and processes contiguous scopes. Segment
  construction is driven by boundary count per scope and active-window checks.
- Built-in segment comparators are linear in aligned segment count.
- Transition comparators build per-scope transition indexes before matching.

Breaking public API changes after freeze should be tied to explicit version
planning.
