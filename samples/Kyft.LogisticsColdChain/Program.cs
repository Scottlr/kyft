using Kyft; // Import Kyft's pipeline and comparison APIs.

var start = DateTimeOffset.Parse("2026-04-21T06:00:00Z"); // Use a deterministic clock.
var pipeline = Kyft.Kyft // Start a Kyft pipeline definition.
    .For<ShipmentTelemetry>() // Consume shipment telemetry events.
    .RecordIntervals() // Record temperature excursion windows.
    .WithEventTime(update => update.Timestamp) // Attach event time to recorded windows.
    .Window("TemperatureExcursion", window => window // Define a segmented window.
        .Key(update => update.ShipmentId) // Track each shipment independently.
        .ActiveWhen(update => update.TemperatureCelsius is < 2.0 or > 8.0) // Open outside the safe range.
        .Segment("leg", segment => segment.Value(update => update.Leg)) // Split windows by transport leg.
        .Segment("custody", segment => segment.Value(update => update.Custody)) // Split windows by custody owner.
        .Tag("productClass", update => update.ProductClass)) // Attach descriptive metadata.
    .Build(); // Build the executable pipeline.

Ingest("sensor-gateway", 0, "warehouse", "carrier-a", 5.0); // Gateway reports safe warehouse storage.
Ingest("carrier-feed", 0, "warehouse", "carrier-a", 5.2); // Carrier feed agrees.
Ingest("sensor-gateway", 20, "road", "carrier-a", 9.4); // Gateway sees a road-leg excursion.
Ingest("carrier-feed", 25, "road", "carrier-a", 9.1); // Carrier feed sees the excursion later.
Ingest("sensor-gateway", 50, "road", "carrier-a", 6.0); // Gateway sees recovery.
Ingest("carrier-feed", 60, "road", "carrier-a", 5.8); // Carrier feed sees recovery later.
Ingest("sensor-gateway", 80, "handoff", "carrier-b", 1.4); // A second excursion begins at custody handoff.

var snapshot = pipeline.Intervals.SnapshotAt(TemporalPoint.ForPosition(8)); // Evaluate the live recorded history.
var byLeg = snapshot.Query() // Query the snapshot.
    .Window("TemperatureExcursion") // Scope to excursions.
    .Windows() // Include final and provisional records.
    .SummarizeBySegment("leg"); // Summarize measured time by transport leg.

var comparison = pipeline.Intervals // Start a source comparison.
    .Compare("Cold-chain telemetry agreement") // Name the comparison.
    .Target("sensor-gateway", selector => selector.Source("sensor-gateway")) // Treat the device gateway as target.
    .Against("carrier-feed", selector => selector.Source("carrier-feed")) // Compare the carrier feed.
    .Within(_ => ComparisonScope.Window("TemperatureExcursion", TemporalAxis.Timestamp).Segment("leg", "road")) // Ask only about road-leg excursions on event time.
    .Normalize(normalization => normalization.OnEventTime()) // Measure the comparison on event time.
    .Using(comparators => comparators.Overlap().Residual().LeadLag(LeadLagTransition.Start, TemporalAxis.Timestamp, TimeSpan.FromMinutes(10).Ticks)) // Compare agreement and detection delay.
    .Run(); // Execute the event-time comparison.

Console.WriteLine("Logistics cold-chain telemetry"); // Print the sample title.
Console.WriteLine("leg summaries: " + byLeg.Count); // Show segment-level summary count.
Console.WriteLine("road overlap rows: " + comparison.OverlapRows.Count); // Show agreement rows.
Console.WriteLine("gateway-only rows: " + comparison.ResidualRows.Count); // Show gateway-only excursion evidence.
Console.WriteLine("lead/lag rows: " + comparison.LeadLagRows.Count); // Show timing drift rows.

void Ingest(string source, int minute, string leg, string custody, double temperature) // Keep sample ingestion compact.
{
    pipeline.Ingest( // Send one telemetry update through Kyft.
        new ShipmentTelemetry( // Create the telemetry event.
            "shipment-2048", // Track one shipment.
            leg, // Store the current transport leg.
            custody, // Store the current custody owner.
            "biomedical-reagent", // Attach a neutral product class.
            temperature, // Carry the measured temperature.
            start.AddMinutes(minute)), // Use deterministic event time.
        source: source); // Store the reporting system as the source lane.
}

public sealed record ShipmentTelemetry( // Define the event shape.
    string ShipmentId, // Identifies the shipment.
    string Leg, // Describes the current transport leg.
    string Custody, // Describes the current custody owner.
    string ProductClass, // Descriptive tag for analysis.
    double TemperatureCelsius, // Drives the excursion predicate.
    DateTimeOffset Timestamp); // Provides event-time bounds.
