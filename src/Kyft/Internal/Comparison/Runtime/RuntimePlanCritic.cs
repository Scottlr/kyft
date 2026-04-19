using Kyft;

namespace Kyft.Internal.Comparison;

internal static class RuntimePlanCritic
{
    internal static ComparisonPlanDiagnostic[] Criticize(PreparedComparison prepared)
    {
        var diagnostics = new List<ComparisonPlanDiagnostic>();
        var plan = prepared.Plan;
        var severity = plan.IsStrict
            ? ComparisonPlanDiagnosticSeverity.Error
            : ComparisonPlanDiagnosticSeverity.Warning;

        AddNonSerializablePlanDiagnostic(plan, severity, diagnostics);
        AddBroadScopeDiagnostic(plan, severity, diagnostics);
        AddFutureLeakageDiagnostic(plan, severity, diagnostics);
        AddLiveFinalityDiagnostic(plan, severity, diagnostics);
        AddUnboundedOpenDurationDiagnostic(prepared, severity, diagnostics);
        AddMixedClockDiagnostic(plan, severity, diagnostics);

        return diagnostics.ToArray();
    }

    private static void AddNonSerializablePlanDiagnostic(
        ComparisonPlan plan,
        ComparisonPlanDiagnosticSeverity severity,
        List<ComparisonPlanDiagnostic> diagnostics)
    {
        if (plan.IsSerializable)
        {
            return;
        }

        diagnostics.Add(new ComparisonPlanDiagnostic(
            ComparisonPlanValidationCode.RuntimeNonSerializablePlan,
            "Runtime-only selectors make this plan hard to audit, export, and replay.",
            "plan",
            severity));
    }

    private static void AddBroadScopeDiagnostic(
        ComparisonPlan plan,
        ComparisonPlanDiagnosticSeverity severity,
        List<ComparisonPlanDiagnostic> diagnostics)
    {
        if (plan.Scope is not null && !string.IsNullOrWhiteSpace(plan.Scope.WindowName))
        {
            return;
        }

        diagnostics.Add(new ComparisonPlanDiagnostic(
            ComparisonPlanValidationCode.BroadSelector,
            "Plan scope is unrestricted; prefer a named window scope for auditable analytics.",
            "scope",
            severity));
    }

    private static void AddFutureLeakageDiagnostic(
        ComparisonPlan plan,
        ComparisonPlanDiagnosticSeverity severity,
        List<ComparisonPlanDiagnostic> diagnostics)
    {
        if (plan.Normalization.KnownAt.HasValue || !HasPointInTimeLookup(plan))
        {
            return;
        }

        diagnostics.Add(new ComparisonPlanDiagnostic(
            ComparisonPlanValidationCode.FutureLeakageRisk,
            "Point-in-time lookup without a known-at point may leak records that were not available at decision time.",
            "normalization.knownAt",
            severity));
    }

    private static void AddLiveFinalityDiagnostic(
        ComparisonPlan plan,
        ComparisonPlanDiagnosticSeverity severity,
        List<ComparisonPlanDiagnostic> diagnostics)
    {
        if (plan.Normalization.OpenWindowPolicy != ComparisonOpenWindowPolicy.ClipToHorizon
            || plan.Normalization.OpenWindowHorizon.HasValue)
        {
            return;
        }

        diagnostics.Add(new ComparisonPlanDiagnostic(
            ComparisonPlanValidationCode.LiveFinalityWithoutHorizon,
            "Open-window clipping requires an explicit evaluation horizon.",
            "normalization.openWindowHorizon",
            severity));
    }

    private static void AddUnboundedOpenDurationDiagnostic(
        PreparedComparison prepared,
        ComparisonPlanDiagnosticSeverity severity,
        List<ComparisonPlanDiagnostic> diagnostics)
    {
        for (var i = 0; i < prepared.ExcludedWindows.Count; i++)
        {
            if (prepared.ExcludedWindows[i].DiagnosticCode != ComparisonPlanValidationCode.OpenWindowsWithoutPolicy)
            {
                continue;
            }

            diagnostics.Add(new ComparisonPlanDiagnostic(
                ComparisonPlanValidationCode.UnboundedOpenDuration,
                "An open window was excluded because the plan did not bound its duration.",
                "normalization.openWindowPolicy",
                severity));
            return;
        }
    }

    private static void AddMixedClockDiagnostic(
        ComparisonPlan plan,
        ComparisonPlanDiagnosticSeverity severity,
        List<ComparisonPlanDiagnostic> diagnostics)
    {
        if (!plan.Normalization.OpenWindowHorizon.HasValue
            || !plan.Normalization.KnownAt.HasValue)
        {
            return;
        }

        var horizon = plan.Normalization.OpenWindowHorizon.Value;
        var knownAt = plan.Normalization.KnownAt.Value;
        if (horizon.Axis != TemporalAxis.Timestamp
            || knownAt.Axis != TemporalAxis.Timestamp
            || string.Equals(horizon.Clock, knownAt.Clock, StringComparison.Ordinal))
        {
            return;
        }

        diagnostics.Add(new ComparisonPlanDiagnostic(
            ComparisonPlanValidationCode.MixedClockRisk,
            "Timestamp horizon and known-at point use different clock identities.",
            "normalization",
            severity));
    }

    private static bool HasPointInTimeLookup(ComparisonPlan plan)
    {
        for (var i = 0; i < plan.Comparators.Count; i++)
        {
            if (plan.Comparators[i].StartsWith("asof:", StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
