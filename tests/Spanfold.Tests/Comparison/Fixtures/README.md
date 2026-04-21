# Spanfold Contract Fixtures

Contract fixtures are small, language-neutral examples for future ports, CLI
checks, and agent verification. They intentionally describe input windows and a
portable comparison plan instead of C# pipeline setup code.

Fixture shape:

- `schema` and `schemaVersion` identify the fixture format.
- `windows` contains closed input windows with `windowName`, `key`, `source`,
  `startPosition`, and `endPosition`.
- `plan` contains a portable subset: `name`, `targetSource`, `againstSources`,
  `scopeWindow`, `comparators`, and `strict`.
- `expected` contains the expected validity, diagnostic codes, comparator
  summaries, and compact row assertions.

The C# tests execute each fixture and compare the exported result JSON against
the expected fixture values.
