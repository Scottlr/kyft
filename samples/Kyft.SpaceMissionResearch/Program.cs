using Kyft; // Import Kyft's temporal analytics APIs.

var start = DateTimeOffset.Parse("2031-11-04T00:00:00Z"); // Use a deterministic mission clock.
var pipeline = Kyft.Kyft // Start a Kyft pipeline definition.
    .For<SpacecraftTelemetry>() // Consume spacecraft subsystem telemetry.
    .RecordIntervals() // Record anomalous-state windows.
    .WithEventTime(sample => sample.Timestamp) // Attach event time.
    .Window("ThermalControlAnomaly", window => window // Define a subsystem anomaly window.
        .Key(sample => sample.ComponentId) // Track each component independently.
        .ActiveWhen(sample => sample.TemperatureKelvin > sample.TemperatureLimitKelvin || sample.CoolantFlowGramsPerSecond < 1.5) // Open on thermal or coolant-flow risk.
        .Segment("missionPhase", segment => segment // Split by mission phase.
            .Value(sample => sample.MissionPhase) // Store the phase value.
            .Child("attitudeMode", child => child.Value(sample => sample.AttitudeMode))) // Split again by attitude mode.
        .Tag("instrument", sample => sample.InstrumentId)) // Attach instrument metadata.
    .RollUp( // Roll component anomalies up to instrument level.
        "InstrumentThermalConstraint", // Name the parent window.
        sample => sample.InstrumentId, // Group components by instrument.
        children => children.ActiveCount >= 2, // Open when at least two components are anomalous.
        segments => segments // Preserve mission context at the parent level.
            .Preserve("missionPhase") // Keep mission phase boundaries.
            .Preserve("attitudeMode")) // Keep attitude-mode boundaries.
    .RollUp( // Roll instrument constraints up to spacecraft level.
        "SpacecraftThermalConstraint", // Name the top-level window.
        sample => sample.SpacecraftId, // Group instruments by spacecraft.
        children => children.ActiveCount > 0, // Open when any instrument is constrained.
        segments => segments.Preserve("missionPhase")) // Keep phase boundaries but drop lower-level attitude splits.
    .Build(); // Build the mission analytics pipeline.

Ingest("thermal-model", 0, "cruise", "sun-point", "camera", "radiator-a", 285, 290, 2.4); // Model starts nominal.
Ingest("flight-telemetry", 0, "cruise", "sun-point", "camera", "radiator-a", 284, 290, 2.5); // Flight telemetry starts nominal.
Ingest("thermal-model", 8, "approach", "slew", "camera", "radiator-a", 296, 290, 1.2); // Model predicts radiator risk.
Ingest("thermal-model", 9, "approach", "slew", "camera", "coolant-loop-a", 291, 290, 1.1); // Model predicts loop risk.
Ingest("flight-telemetry", 12, "approach", "slew", "camera", "radiator-a", 297, 290, 1.3); // Flight telemetry observes radiator risk.
Ingest("flight-telemetry", 14, "approach", "slew", "camera", "coolant-loop-a", 292, 290, 1.2); // Flight telemetry observes loop risk.
Ingest("thermal-model", 25, "approach", "science-point", "camera", "radiator-a", 287, 290, 2.1); // Model clears one component.
Ingest("thermal-model", 26, "approach", "science-point", "camera", "coolant-loop-a", 286, 290, 2.2); // Model clears the second component.
Ingest("flight-telemetry", 32, "approach", "science-point", "camera", "radiator-a", 288, 290, 2.0); // Flight telemetry clears one component.
Ingest("flight-telemetry", 34, "approach", "science-point", "camera", "coolant-loop-a", 287, 290, 2.2); // Flight telemetry clears the second component.
Ingest("thermal-model", 45, "orbital-insertion", "burn", "spectrometer", "detector-b", 305, 300, 1.0); // Model opens a new burn-phase anomaly.
Ingest("thermal-model", 46, "orbital-insertion", "burn", "spectrometer", "cooler-b", 302, 300, 1.1); // Model opens an instrument-level burn constraint.

var instrumentRows = pipeline.Intervals // Compare instrument-level constraints.
    .Compare("Thermal model versus flight telemetry") // Name the comparison.
    .Target("thermal-model", selector => selector.Source("thermal-model")) // Treat model output as target.
    .Against("flight-telemetry", selector => selector.Source("flight-telemetry")) // Compare observed telemetry.
    .Within(_ => ComparisonScope.Window("InstrumentThermalConstraint", TemporalAxis.Timestamp).Segment("missionPhase", "approach")) // Analyze approach-phase instrument constraints on event time.
    .Normalize(normalization => normalization.OnEventTime()) // Measure the comparison on event time.
    .Using(comparators => comparators.Overlap().Residual().LeadLag(LeadLagTransition.Start, TemporalAxis.Timestamp, TimeSpan.FromMinutes(10).Ticks).Containment()) // Measure agreement, model-only risk, timing, and containment.
    .Run(); // Execute the comparison.

var spacecraftLive = pipeline.Intervals // Compare top-level spacecraft constraints live.
    .Compare("Live spacecraft thermal constraint") // Name the live comparison.
    .Target("thermal-model", selector => selector.Source("thermal-model")) // Treat model output as target.
    .Against("flight-telemetry", selector => selector.Source("flight-telemetry")) // Compare observed telemetry.
    .Within(scope => scope.Window("SpacecraftThermalConstraint")) // Analyze spacecraft-level constraints.
    .Using(comparators => comparators.Residual().Coverage()) // Measure model-only live constraints.
    .RunLive(TemporalPoint.ForPosition(13)); // Clip the burn-phase model anomaly to a live horizon.

var snapshot = pipeline.Intervals.SnapshotAt(TemporalPoint.ForPosition(13)); // Build a horizon snapshot.
var phaseSummary = snapshot.Query() // Query the horizon snapshot.
    .Window("ThermalControlAnomaly") // Read component-level anomaly windows.
    .Windows() // Include final and provisional records.
    .SummarizeBySegment("missionPhase"); // Summarize by mission phase.

var liveAnomaly = pipeline.Intervals.Query() // Start a direct query.
    .Window("ThermalControlAnomaly") // Scope to component anomalies.
    .Lane("thermal-model") // Read model-generated windows.
    .LatestWindowAt(TemporalPoint.ForPosition(13)); // Get the latest model anomaly at the horizon.

if (liveAnomaly is not null) // Guard the annotation sample.
{
    pipeline.Intervals.Annotate( // Attach an explanatory research note.
        liveAnomaly.Window, // Annotate the source window.
        "analysisNote", // Name the annotation.
        "burn attitude constraint requires follow-up thermal vacuum model", // Store a neutral research note.
        TemporalPoint.ForPosition(13)); // Record when the note became known.
}

Console.WriteLine("Space mission research-grade thermal analysis"); // Print the sample title.
Console.WriteLine("instrument overlap rows: " + instrumentRows.OverlapRows.Count); // Show model/telemetry agreement.
Console.WriteLine("instrument residual rows: " + instrumentRows.ResidualRows.Count); // Show model-only intervals.
Console.WriteLine("lead/lag rows: " + instrumentRows.LeadLagRows.Count); // Show timing drift.
Console.WriteLine("spacecraft provisional rows: " + spacecraftLive.ProvisionalRowFinalities().Count); // Show live rows.
Console.WriteLine("phase summaries: " + phaseSummary.Count); // Show snapshot grouping.
Console.WriteLine("latest live anomaly annotations: " + (liveAnomaly is null ? 0 : pipeline.Intervals.AnnotationsFor(liveAnomaly.Window).Count)); // Show annotation count.

void Ingest( // Keep mission telemetry ingestion compact.
    string source, // The source lane.
    int minute, // The minute offset.
    string missionPhase, // The mission phase.
    string attitudeMode, // The attitude mode.
    string instrument, // The instrument key.
    string component, // The component key.
    double temperature, // The component temperature.
    double limit, // The component temperature limit.
    double coolantFlow) // The coolant flow value.
{
    pipeline.Ingest( // Send one spacecraft sample through Kyft.
        new SpacecraftTelemetry( // Create the telemetry event.
            "voyager-research-analogue", // Use a fictional neutral spacecraft id.
            instrument, // Attach instrument context.
            component, // Track the component.
            missionPhase, // Split by mission phase.
            attitudeMode, // Split by attitude mode.
            temperature, // Carry measured or modeled temperature.
            limit, // Carry the component limit.
            coolantFlow, // Carry coolant-flow estimate.
            start.AddMinutes(minute)), // Use deterministic event time.
        source: source); // Store model or telemetry as the source lane.
}

public sealed record SpacecraftTelemetry( // Define the mission telemetry event.
    string SpacecraftId, // Identifies the spacecraft.
    string InstrumentId, // Identifies the instrument.
    string ComponentId, // Identifies the component.
    string MissionPhase, // Splits windows by mission phase.
    string AttitudeMode, // Splits windows by spacecraft attitude mode.
    double TemperatureKelvin, // Contributes to the anomaly predicate.
    double TemperatureLimitKelvin, // Provides the thermal limit.
    double CoolantFlowGramsPerSecond, // Contributes to the anomaly predicate.
    DateTimeOffset Timestamp); // Provides event-time bounds.
