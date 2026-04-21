# Kyft Samples

The sample projects are ordered from simple recorded-window usage to more
advanced temporal analysis.

```bash
dotnet run --project samples/Kyft.SimpleDeviceMonitor/Kyft.SimpleDeviceMonitor.csproj # Basic source comparison and live horizon query.
dotnet run --project samples/Kyft.LogisticsColdChain/Kyft.LogisticsColdChain.csproj # Segment-aware cold-chain telemetry analysis.
dotnet run --project samples/Kyft.SecurityAccessAudit/Kyft.SecurityAccessAudit.csproj # Known-at audit and late annotations.
dotnet run --project samples/Kyft.IndustrialTelemetry/Kyft.IndustrialTelemetry.csproj # Industrial process windows plus lane liveness.
dotnet run --project samples/Kyft.DistributedQuorum/Kyft.DistributedQuorum.csproj # Cohort and source-matrix analysis for replicated systems.
dotnet run --project samples/Kyft.SpaceMissionResearch/Kyft.SpaceMissionResearch.csproj # Hierarchical advanced spacecraft thermal analysis.
dotnet run --project samples/Kyft.OperationsExample/Kyft.OperationsExample.csproj # Service monitoring comparison with exports.
```

Each sample keeps its event model local to the project. The examples are meant
to show Kyft patterns rather than prescribe a domain schema.

The programs print small text diagrams before running the analysis. Those
diagrams explain the temporal setup visually, while the C# stays close to how a
consumer would write real pipeline code.
