using Kyft; // Import Kyft's window and liveness APIs.

var start = DateTimeOffset.Parse("2026-04-21T08:00:00Z"); // Use a deterministic clock.
var processPipeline = Kyft.Kyft // Start a process telemetry pipeline.
    .For<PumpTelemetry>() // Consume pump telemetry.
    .RecordIntervals() // Record abnormal-state windows.
    .WithEventTime(update => update.Timestamp) // Attach event time.
    .Window("PumpCavitationRisk", window => window // Define a process-risk window.
        .Key(update => update.PumpId) // Track each pump independently.
        .ActiveWhen(update => update.VibrationMmPerSecond > 8.0 && update.IntakePressureBar < 1.8) // Open when vibration and pressure imply cavitation risk.
        .Segment("operatingMode", segment => segment.Value(update => update.OperatingMode)) // Split by operating mode.
        .Tag("line", update => update.LineId)) // Attach line metadata.
    .Build(); // Build the process pipeline.

var livenessPipeline = Kyft.Kyft // Start a liveness pipeline.
    .For<LaneLivenessSignal>() // Consume liveness state changes.
    .RecordIntervals() // Record silence windows.
    .WithEventTime(signal => signal.OccurredAt) // Use the actual silence/recovery time.
    .TrackWindow("SensorSilent", window => window // Define a sensor silence window.
        .Key(signal => signal.Lane) // Track each sensor lane independently.
        .ActiveWhen(signal => signal.IsSilent)); // Open while the lane is silent.

var liveness = LaneLivenessTracker.ForLanes( // Create deterministic lane liveness state.
    start, // Start tracking at the process start.
    TimeSpan.FromSeconds(45), // Treat 45 seconds without reports as silence.
    "sensor-a", // Track sensor A.
    "sensor-b"); // Track sensor B.

Observe("sensor-a", 0); // Sensor A reports initially.
Observe("sensor-b", 0); // Sensor B reports initially.
IngestPump("sensor-a", 0, 4.2, 2.5, "steady"); // Sensor A reports normal pump conditions.
IngestPump("sensor-b", 0, 4.5, 2.4, "steady"); // Sensor B agrees.
IngestPump("sensor-a", 1, 9.5, 1.5, "ramp-up"); // Sensor A sees cavitation risk.
CheckLiveness(2); // Horizon check may open silence windows for stale sensors.
Observe("sensor-b", 3); // Sensor B recovers from silence.
IngestPump("sensor-b", 3, 9.1, 1.4, "ramp-up"); // Sensor B now observes the same risk.
IngestPump("sensor-a", 4, 5.0, 2.3, "steady"); // Sensor A sees recovery.
IngestPump("sensor-b", 5, 4.9, 2.4, "steady"); // Sensor B sees recovery.

var riskComparison = processPipeline.Intervals // Start a process-risk comparison.
    .Compare("Pump risk sensor agreement") // Name the comparison.
    .Target("sensor-a", selector => selector.Source("sensor-a")) // Treat sensor A as target.
    .Against("sensor-b", selector => selector.Source("sensor-b")) // Compare sensor B.
    .Within(_ => ComparisonScope.Window("PumpCavitationRisk", TemporalAxis.Timestamp)) // Scope to cavitation risk windows on event time.
    .Normalize(normalization => normalization.OnEventTime()) // Measure the comparison on event time.
    .Using(comparators => comparators.Overlap().Residual().LeadLag(LeadLagTransition.Start, TemporalAxis.Timestamp, TimeSpan.FromMinutes(3).Ticks)) // Compare agreement and detection lag.
    .Run(); // Execute the comparison.

var silence = livenessPipeline.Intervals.Query() // Start a liveness history query.
    .Window("SensorSilent") // Read sensor silence windows.
    .ClosedWindows(); // Materialize closed silence windows.

Console.WriteLine("Industrial telemetry"); // Print the sample title.
Console.WriteLine("risk overlap rows: " + riskComparison.OverlapRows.Count); // Show process-risk agreement.
Console.WriteLine("risk lead/lag rows: " + riskComparison.LeadLagRows.Count); // Show detection timing rows.
Console.WriteLine("closed silence windows: " + silence.Count); // Show sensor liveness gaps.

void Observe(string lane, int minute) // Record a sensor heartbeat.
{
    foreach (var signal in liveness.Observe(lane, start.AddMinutes(minute))) // Emit liveness state changes.
    {
        livenessPipeline.Ingest(signal, source: "liveness"); // Ingest state changes into the liveness pipeline.
    }
}

void CheckLiveness(int minute) // Evaluate liveness at a horizon.
{
    foreach (var signal in liveness.Check(start.AddMinutes(minute))) // Emit silence state changes.
    {
        livenessPipeline.Ingest(signal, source: "liveness"); // Ingest silence transitions.
    }
}

void IngestPump(string source, int minute, double vibration, double pressure, string mode) // Record process telemetry.
{
    processPipeline.Ingest( // Send one pump update through Kyft.
        new PumpTelemetry( // Create the telemetry event.
            "line-3", // Attach line context.
            "pump-4", // Track one pump.
            mode, // Carry operating mode.
            vibration, // Carry vibration magnitude.
            pressure, // Carry intake pressure.
            start.AddMinutes(minute)), // Use deterministic event time.
        source: source); // Store the sensor as the source lane.
}

public sealed record PumpTelemetry( // Define process telemetry.
    string LineId, // Identifies the production line.
    string PumpId, // Identifies the pump.
    string OperatingMode, // Splits risk windows by mode.
    double VibrationMmPerSecond, // Contributes to the active predicate.
    double IntakePressureBar, // Contributes to the active predicate.
    DateTimeOffset Timestamp); // Provides event-time bounds.
