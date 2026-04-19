# Fixture Schema

Kyft CLI fixtures are compact JSON files for portable comparison examples and
agent-readable regression cases.

## Shape

JSON examples are left uncommented because JSON fixtures must remain valid JSON.

```json
{
  "schema": "kyft.contract-fixture",
  "schemaVersion": 1,
  "name": "basic-overlap",
  "windows": [
    {
      "windowName": "DeviceOffline",
      "key": "device-1",
      "source": "provider-a",
      "partition": "fleet-a",
      "startPosition": 1,
      "endPosition": 5
    }
  ],
  "plan": {
    "name": "Provider QA",
    "targetSource": "provider-a",
    "againstSources": [ "provider-b" ],
    "scopeWindow": "DeviceOffline",
    "comparators": [ "overlap", "residual", "coverage" ],
    "strict": false
  }
}
```

## Open Windows

Use `null` for `endPosition` when a window is still open:

```json
{
  "windowName": "DeviceOffline",
  "key": "device-1",
  "source": "provider-a",
  "startPosition": 10,
  "endPosition": null
}
```

Historical runs keep Kyft's normal safety behavior. Live fixture runs should add
`liveHorizonPosition` to the plan.

## Commands

```bash
dotnet run --project src/Kyft.Cli/Kyft.Cli.csproj -- validate-plan fixture.json # Validate the fixture plan.
dotnet run --project src/Kyft.Cli/Kyft.Cli.csproj -- compare fixture.json --format json # Execute and export JSON.
dotnet run --project src/Kyft.Cli/Kyft.Cli.csproj -- explain fixture.json # Execute and export Markdown.
```

The CLI validates required fixture properties before execution and returns JSON
diagnostics on standard error for malformed fixtures.
