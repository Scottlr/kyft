# Kyft Samples

The sample projects are ordered from simple recorded-window usage to more
advanced temporal analysis.

The samples avoid domain-specific package assumptions. Each project keeps a
small event type in `Program.cs`, records window history with `RecordWindows()`,
then queries or compares `pipeline.History`.

## Running A Sample

```bash
dotnet run --project samples/Kyft.SimpleDeviceMonitor/Kyft.SimpleDeviceMonitor.csproj # Basic source comparison and live horizon query.
dotnet run --project samples/Kyft.LogisticsColdChain/Kyft.LogisticsColdChain.csproj # Segment-aware cold-chain telemetry analysis.
dotnet run --project samples/Kyft.SecurityAccessAudit/Kyft.SecurityAccessAudit.csproj # Known-at audit and late annotations.
dotnet run --project samples/Kyft.IndustrialTelemetry/Kyft.IndustrialTelemetry.csproj # Industrial process windows plus lane liveness.
dotnet run --project samples/Kyft.DistributedQuorum/Kyft.DistributedQuorum.csproj # Cohort and source-matrix analysis for replicated systems.
dotnet run --project samples/Kyft.SpaceMissionResearch/Kyft.SpaceMissionResearch.csproj # Hierarchical advanced spacecraft thermal analysis.
dotnet run --project samples/Kyft.OperationsExample/Kyft.OperationsExample.csproj # Service monitoring comparison with exports.
```

## Reading Order

1. `Kyft.SimpleDeviceMonitor` introduces the smallest useful shape: record
   windows, inspect direct history, then compare two sources.
2. `Kyft.LogisticsColdChain` adds lifecycle segmentation and horizon snapshots.
3. `Kyft.SecurityAccessAudit` shows late annotations and known-at reads.
4. `Kyft.IndustrialTelemetry` separates process-state windows from liveness
   windows, then compares the resulting histories.
5. `Kyft.DistributedQuorum` uses cohort comparison and source matrices to
   reason about replicated observations.
6. `Kyft.SpaceMissionResearch` combines hierarchy, cohorts, tags, live windows,
   snapshots, and annotations in a more demanding analysis.
7. `Kyft.OperationsExample` focuses on practical export workflows.

## What To Notice

Most samples follow the same flow:

```text
events -> pipeline -> recorded window history -> query / compare / export
```

Kyft is strongest when the question is about recorded temporal state:

- when a state was active
- which source observed it
- which source missed it
- how windows overlap, diverge, or remain open at a horizon
- how segments and tags make the same history readable from several angles

Each sample keeps its event model local to the project. The examples are meant
to show Kyft patterns rather than prescribe a domain schema.

The programs print small text diagrams before running the analysis. Those
diagrams explain the temporal setup visually, while the C# stays close to how a
consumer would write real pipeline code.
