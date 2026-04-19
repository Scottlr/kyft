# Fixture Schema

Kyft CLI fixtures are compact JSON files for portable comparison examples and
agent-readable regression cases.

## Shape

```json
{
  "schema": "kyft.contract-fixture",
  "schemaVersion": 1,
  "name": "basic-overlap",
  "windows": [
    {
      "windowName": "SelectionUnavailable",
      "key": "home-win",
      "source": "provider-a",
      "partition": "fixture-101",
      "startPosition": 1,
      "endPosition": 5
    }
  ],
  "plan": {
    "name": "Provider QA",
    "targetSource": "provider-a",
    "againstSources": [ "provider-b" ],
    "scopeWindow": "SelectionUnavailable",
    "comparators": [ "overlap", "residual", "coverage" ],
    "strict": false
  }
}
```

## Open Windows

Use `null` for `endPosition` when a window is still open:

```json
{
  "windowName": "SelectionUnavailable",
  "key": "home-win",
  "source": "provider-a",
  "startPosition": 10,
  "endPosition": null
}
```

Historical runs keep Kyft's normal safety behavior. Live fixture runs should add
`liveHorizonPosition` to the plan.

## Commands

```bash
dotnet run --project src/Kyft.Cli/Kyft.Cli.csproj -- validate-plan fixture.json
dotnet run --project src/Kyft.Cli/Kyft.Cli.csproj -- compare fixture.json --format json
dotnet run --project src/Kyft.Cli/Kyft.Cli.csproj -- explain fixture.json
```

The CLI validates required fixture properties before execution and returns JSON
diagnostics on standard error for malformed fixtures.
