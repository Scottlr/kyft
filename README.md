# Spanfold

Spanfold records temporal state windows and compares them across sources,
providers, lanes, or pipeline stages.

The repository is organized by language package:

- [.NET / C#](packages/dotnet/README.md) - reference implementation, CLI, docs,
  samples, tests, and benchmarks.
- [Python](packages/python/README.md) - partial typed port.
- [Pages site](pages/index.html) - static GitHub Pages deploy target.

## Repository Layout

```text
packages/
  dotnet/
    src/
    tests/
    samples/
    benchmarks/
    docs/
    Spanfold.slnx
  python/
    src/
    tests/
    samples/
    docs/
    pyproject.toml
pages/
  index.html
  assets/
```

## Website

The GitHub Pages site is served from the root-level `pages/` folder by
`.github/workflows/pages.yml`. Package-local documentation stays with each
language implementation under `packages/<language>/docs`.

## Working With Packages

Run the reference package tests:

```bash
dotnet test packages/dotnet/Spanfold.slnx
```

Run the Python port tests:

```bash
cd packages/python
python -m pip install -e ".[dev]"
pytest
```

Package-specific documentation lives with each language implementation so future
ports can be added under `packages/<language>` without sharing build metadata.
