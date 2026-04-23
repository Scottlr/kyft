use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::Value;

use crate::{
    ComparisonExtensionMetadata, PrimitiveValue, TemporalAxis, WindowHistory, WindowSegment,
    WindowTag,
};

/// Comparator family supported by the Rust implementation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Comparator {
    /// Overlap rows where target and comparison are both active.
    Overlap,
    /// Residual target-only rows.
    Residual,
    /// Missing comparison-only rows.
    Missing,
    /// Coverage rows across target segments.
    Coverage,
    /// Gap rows for observed uncovered spans.
    Gap,
    /// Symmetric-difference rows for disagreement spans.
    SymmetricDifference,
    /// Containment rows for target segments relative to comparison coverage.
    Containment,
    /// Lead/lag measurements over target and comparison transitions.
    LeadLag {
        /// Transition point to compare.
        transition: LeadLagTransition,
        /// Temporal axis for the measurement.
        axis: TemporalAxis,
        /// Allowed delta magnitude.
        tolerance_magnitude: i64,
    },
    /// As-of point-in-time lookup.
    AsOf {
        /// Lookup direction.
        direction: AsOfDirection,
        /// Temporal axis for lookup.
        axis: TemporalAxis,
        /// Allowed match distance.
        tolerance_magnitude: i64,
    },
}

impl Comparator {
    /// Parses a comparator declaration.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "overlap" => Some(Self::Overlap),
            "residual" => Some(Self::Residual),
            "missing" => Some(Self::Missing),
            "coverage" => Some(Self::Coverage),
            "gap" => Some(Self::Gap),
            "symmetric-difference" => Some(Self::SymmetricDifference),
            "containment" => Some(Self::Containment),
            _ => parse_parameterized_comparator(value),
        }
    }

    /// Returns the comparator declaration used in exports.
    #[must_use]
    pub fn declaration(&self) -> String {
        match self {
            Self::Overlap => "overlap".to_owned(),
            Self::Residual => "residual".to_owned(),
            Self::Missing => "missing".to_owned(),
            Self::Coverage => "coverage".to_owned(),
            Self::Gap => "gap".to_owned(),
            Self::SymmetricDifference => "symmetric-difference".to_owned(),
            Self::Containment => "containment".to_owned(),
            Self::LeadLag {
                transition,
                axis,
                tolerance_magnitude,
            } => {
                format!("lead-lag:{transition:?}:{axis:?}:{tolerance_magnitude}")
            }
            Self::AsOf {
                direction,
                axis,
                tolerance_magnitude,
            } => format!("asof:{direction:?}:{axis:?}:{tolerance_magnitude}"),
        }
    }
}

/// Comparison-side selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AgainstSelection {
    /// One or more source lanes.
    Sources(Vec<String>),
    /// Cohort activity across sources.
    Cohort {
        /// Exported cohort name.
        name: String,
        /// Participating sources.
        sources: Vec<String>,
        /// Activity rule.
        activity: CohortActivity,
    },
}

/// Cohort activity rule.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CohortActivity {
    /// Any active source makes the cohort active.
    Any,
    /// All declared sources must be active.
    All,
    /// No declared source may be active.
    None,
    /// At least `count` sources must be active.
    AtLeast {
        /// Required active-member count.
        count: usize,
    },
    /// At most `count` sources may be active.
    AtMost {
        /// Maximum active-member count.
        count: usize,
    },
    /// Exactly `count` sources must be active.
    Exactly {
        /// Exact active-member count.
        count: usize,
    },
}

impl CohortActivity {
    /// Returns the export rule name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::All => "all",
            Self::None => "none",
            Self::AtLeast { .. } => "at-least",
            Self::AtMost { .. } => "at-most",
            Self::Exactly { .. } => "exactly",
        }
    }

    /// Returns the configured threshold, when any.
    #[must_use]
    pub const fn count(&self) -> Option<usize> {
        match self {
            Self::Any | Self::All | Self::None => None,
            Self::AtLeast { count } | Self::AtMost { count } | Self::Exactly { count } => {
                Some(*count)
            }
        }
    }

    /// Evaluates activity for a given active-member count.
    #[must_use]
    pub fn is_active(&self, active_count: usize, member_count: usize) -> bool {
        match self {
            Self::Any => active_count >= 1,
            Self::All => active_count == member_count,
            Self::None => active_count == 0,
            Self::AtLeast { count } => active_count >= *count,
            Self::AtMost { count } => active_count <= *count,
            Self::Exactly { count } => active_count == *count,
        }
    }
}

/// Equality filter over tags or segments.
#[derive(Clone, Debug, PartialEq)]
pub struct WindowFilter {
    /// Filter name.
    pub name: String,
    /// Filter value.
    pub value: PrimitiveValue,
}

/// Typed comparison plan.
#[derive(Clone, Debug, PartialEq)]
pub struct ComparisonPlan {
    /// Comparison name.
    pub name: String,
    /// Target source.
    pub target_source: String,
    /// Comparison side selection.
    pub against: AgainstSelection,
    /// Optional window family scope.
    pub scope_window: Option<String>,
    /// Segment filters.
    pub scope_segments: Vec<WindowFilter>,
    /// Tag filters.
    pub scope_tags: Vec<WindowFilter>,
    /// Comparator declarations.
    pub comparators: Vec<Comparator>,
    /// Availability point used for known-at filtering.
    pub known_at: Option<crate::TemporalPoint>,
    /// How open windows are handled.
    pub open_window_policy: OpenWindowPolicy,
    /// Exclusive horizon used when clipping open windows.
    pub open_window_horizon: Option<crate::TemporalPoint>,
    /// Whether strict validation is enabled.
    pub strict: bool,
}

/// Open-window normalization policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenWindowPolicy {
    /// Open windows are rejected.
    RequireClosed,
    /// Open windows are clipped to the configured horizon.
    ClipToHorizon,
}

/// Diagnostic severity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum DiagnosticSeverity {
    /// Warning-level diagnostic.
    Warning,
    /// Error-level diagnostic.
    Error,
}

/// Structured comparison diagnostic.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ComparisonDiagnostic {
    /// Diagnostic code.
    pub code: String,
    /// Diagnostic severity.
    pub severity: DiagnosticSeverity,
}

/// Comparator summary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ComparatorSummary {
    /// Comparator name.
    #[serde(rename = "comparatorName")]
    pub comparator_name: String,
    /// Row count.
    #[serde(rename = "rowCount")]
    pub row_count: usize,
}

/// Exported range for a row.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RowRange {
    /// Inclusive start magnitude.
    pub start: i64,
    /// Exclusive end magnitude.
    pub end: i64,
}

/// Exported point for transition-based rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub struct RowPoint {
    /// Point axis.
    pub axis: TemporalAxis,
    /// Scalar point magnitude.
    pub magnitude: i64,
}

/// The active side for a disagreement segment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum ComparisonSide {
    /// Target side.
    Target,
    /// Comparison side.
    Against,
}

/// Containment classification for one target-active segment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum ContainmentStatus {
    /// Segment is covered by at least one comparison window.
    Contained,
    /// Segment is not covered by comparison windows.
    NotContained,
    /// Segment starts at the left edge of the target without coverage.
    LeftOverhang,
    /// Segment ends at the right edge of the target without coverage.
    RightOverhang,
}

/// Transition point used for lead/lag measurement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum LeadLagTransition {
    /// Compare start transitions.
    Start,
    /// Compare end transitions.
    End,
}

/// Lead/lag direction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum LeadLagDirection {
    /// Target and comparison are equal.
    Equal,
    /// Target transition occurs first.
    TargetLeads,
    /// Target transition occurs later.
    TargetLags,
    /// No comparison transition exists.
    MissingComparison,
}

/// Summary for one lead/lag comparator declaration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LeadLagSummary {
    /// Transition point measured.
    #[serde(rename = "transition")]
    pub transition: LeadLagTransition,
    /// Axis used for measurement.
    #[serde(rename = "axis")]
    pub axis: TemporalAxis,
    /// Configured tolerance.
    #[serde(rename = "toleranceMagnitude")]
    pub tolerance_magnitude: i64,
    /// Number of emitted rows.
    #[serde(rename = "rowCount")]
    pub row_count: usize,
    /// Count of target-lead rows.
    #[serde(rename = "targetLeadCount")]
    pub target_lead_count: usize,
    /// Count of target-lag rows.
    #[serde(rename = "targetLagCount")]
    pub target_lag_count: usize,
    /// Count of equal rows.
    #[serde(rename = "equalCount")]
    pub equal_count: usize,
    /// Count of missing-comparison rows.
    #[serde(rename = "missingComparisonCount")]
    pub missing_comparison_count: usize,
    /// Count of rows outside tolerance.
    #[serde(rename = "outsideToleranceCount")]
    pub outside_tolerance_count: usize,
    /// Minimum signed delta when any paired transitions exist.
    #[serde(rename = "minimumDeltaMagnitude")]
    pub minimum_delta_magnitude: Option<i64>,
    /// Maximum signed delta when any paired transitions exist.
    #[serde(rename = "maximumDeltaMagnitude")]
    pub maximum_delta_magnitude: Option<i64>,
}

/// Coverage summary for one comparison scope.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CoverageSummary {
    /// Window family.
    #[serde(rename = "windowName")]
    pub window_name: String,
    /// Logical key.
    pub key: String,
    /// Optional partition.
    pub partition: Option<String>,
    /// Denominator magnitude.
    #[serde(rename = "targetMagnitude")]
    pub target_magnitude: f64,
    /// Covered numerator magnitude.
    #[serde(rename = "coveredMagnitude")]
    pub covered_magnitude: f64,
    /// Covered ratio.
    #[serde(rename = "coverageRatio")]
    pub coverage_ratio: f64,
}

/// Finality state for an emitted row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum ComparisonFinality {
    /// Row is final.
    Final,
    /// Row depends on clipped open windows.
    Provisional,
    /// Row supersedes a prior version.
    Revised,
    /// Row was removed in a later snapshot.
    Retracted,
}

/// Finality metadata for a materialized row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ComparisonRowFinality {
    /// Exported row family.
    #[serde(rename = "rowType")]
    pub row_type: String,
    /// Deterministic row identifier.
    #[serde(rename = "rowId")]
    pub row_id: String,
    /// Finality state.
    pub finality: ComparisonFinality,
    /// Human-readable reason.
    pub reason: String,
    /// Metadata version.
    pub version: u32,
    /// Superseded row identifier, when any.
    #[serde(rename = "supersedesRowId")]
    pub supersedes_row_id: Option<String>,
}

/// As-of lookup direction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum AsOfDirection {
    /// Match the latest comparison transition at or before the target point.
    Previous,
    /// Match the earliest comparison transition at or after the target point.
    Next,
    /// Match the nearest comparison transition on either side.
    Nearest,
}

/// As-of lookup status.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum AsOfMatchStatus {
    /// Exact point match.
    Exact,
    /// Matched within tolerance.
    Matched,
    /// No match inside tolerance.
    NoMatch,
    /// A future point existed but was rejected.
    FutureRejected,
    /// Multiple eligible matches existed; selection is deterministic.
    Ambiguous,
}

/// Overlap row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct OverlapRow {
    /// Window family.
    #[serde(rename = "windowName")]
    pub window_name: String,
    /// Logical key.
    pub key: String,
    /// Optional partition.
    pub partition: Option<String>,
    /// Overlap range.
    pub range: RowRange,
    /// Target record IDs.
    #[serde(rename = "targetRecordIds")]
    pub target_record_ids: Vec<String>,
    /// Against record IDs.
    #[serde(rename = "againstRecordIds")]
    pub against_record_ids: Vec<String>,
}

/// Residual row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ResidualRow {
    /// Window family.
    #[serde(rename = "windowName")]
    pub window_name: String,
    /// Logical key.
    pub key: String,
    /// Optional partition.
    pub partition: Option<String>,
    /// Target-only range.
    pub range: RowRange,
    /// Target record IDs.
    #[serde(rename = "targetRecordIds")]
    pub target_record_ids: Vec<String>,
}

/// Missing row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct MissingRow {
    /// Window family.
    #[serde(rename = "windowName")]
    pub window_name: String,
    /// Logical key.
    pub key: String,
    /// Optional partition.
    pub partition: Option<String>,
    /// Comparison-only range.
    pub range: RowRange,
    /// Against record IDs.
    #[serde(rename = "againstRecordIds")]
    pub against_record_ids: Vec<String>,
}

/// Coverage row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CoverageRow {
    /// Window family.
    #[serde(rename = "windowName")]
    pub window_name: String,
    /// Logical key.
    pub key: String,
    /// Optional partition.
    pub partition: Option<String>,
    /// Target segment range.
    pub range: RowRange,
    /// Segment magnitude.
    #[serde(rename = "targetMagnitude")]
    pub target_magnitude: i64,
    /// Covered magnitude.
    #[serde(rename = "coveredMagnitude")]
    pub covered_magnitude: i64,
    /// Target record IDs.
    #[serde(rename = "targetRecordIds")]
    pub target_record_ids: Vec<String>,
    /// Against record IDs.
    #[serde(rename = "againstRecordIds")]
    pub against_record_ids: Vec<String>,
}

/// Gap row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct GapRow {
    /// Window family.
    #[serde(rename = "windowName")]
    pub window_name: String,
    /// Logical key.
    pub key: String,
    /// Optional partition.
    pub partition: Option<String>,
    /// Gap range.
    pub range: RowRange,
}

/// Symmetric-difference row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SymmetricDifferenceRow {
    /// Window family.
    #[serde(rename = "windowName")]
    pub window_name: String,
    /// Logical key.
    pub key: String,
    /// Optional partition.
    pub partition: Option<String>,
    /// Disagreement range.
    pub range: RowRange,
    /// Active disagreement side.
    pub side: ComparisonSide,
    /// Target record IDs.
    #[serde(rename = "targetRecordIds")]
    pub target_record_ids: Vec<String>,
    /// Against record IDs.
    #[serde(rename = "againstRecordIds")]
    pub against_record_ids: Vec<String>,
}

/// Containment row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ContainmentRow {
    /// Window family.
    #[serde(rename = "windowName")]
    pub window_name: String,
    /// Logical key.
    pub key: String,
    /// Optional partition.
    pub partition: Option<String>,
    /// Checked range.
    pub range: RowRange,
    /// Containment status.
    pub status: ContainmentStatus,
    /// Target record IDs.
    #[serde(rename = "targetRecordIds")]
    pub target_record_ids: Vec<String>,
    /// Container record IDs.
    #[serde(rename = "containerRecordIds")]
    pub container_record_ids: Vec<String>,
}

/// Lead/lag row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct LeadLagRow {
    /// Window family.
    #[serde(rename = "windowName")]
    pub window_name: String,
    /// Logical key.
    pub key: String,
    /// Optional partition.
    pub partition: Option<String>,
    /// Measured transition.
    pub transition: LeadLagTransition,
    /// Measurement axis.
    pub axis: TemporalAxis,
    /// Target transition point.
    #[serde(rename = "targetPoint")]
    pub target_point: RowPoint,
    /// Comparison transition point, when any.
    #[serde(rename = "comparisonPoint")]
    pub comparison_point: Option<RowPoint>,
    /// Signed target-minus-comparison delta.
    #[serde(rename = "deltaMagnitude")]
    pub delta_magnitude: Option<i64>,
    /// Configured tolerance.
    #[serde(rename = "toleranceMagnitude")]
    pub tolerance_magnitude: i64,
    /// Whether the row is inside tolerance.
    #[serde(rename = "isWithinTolerance")]
    pub is_within_tolerance: bool,
    /// Lead/lag direction.
    pub direction: LeadLagDirection,
    /// Target record ID.
    #[serde(rename = "targetRecordId")]
    pub target_record_id: String,
    /// Comparison record ID, when any.
    #[serde(rename = "comparisonRecordId")]
    pub comparison_record_id: Option<String>,
}

/// As-of row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AsOfRow {
    /// Window family.
    #[serde(rename = "windowName")]
    pub window_name: String,
    /// Logical key.
    pub key: String,
    /// Optional partition.
    pub partition: Option<String>,
    /// Lookup axis.
    pub axis: TemporalAxis,
    /// Lookup direction.
    pub direction: AsOfDirection,
    /// Target lookup point.
    #[serde(rename = "targetPoint")]
    pub target_point: RowPoint,
    /// Matched comparison point, when any.
    #[serde(rename = "matchedPoint")]
    pub matched_point: Option<RowPoint>,
    /// Absolute point distance, when evaluated.
    #[serde(rename = "distanceMagnitude")]
    pub distance_magnitude: Option<i64>,
    /// Configured tolerance.
    #[serde(rename = "toleranceMagnitude")]
    pub tolerance_magnitude: i64,
    /// Match status.
    pub status: AsOfMatchStatus,
    /// Target record ID.
    #[serde(rename = "targetRecordId")]
    pub target_record_id: String,
    /// Matched comparison record ID, when any.
    #[serde(rename = "matchedRecordId")]
    pub matched_record_id: Option<String>,
}

/// Comparator row collections.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct ComparisonRows {
    /// Overlap rows.
    pub overlap: Vec<OverlapRow>,
    /// Residual rows.
    pub residual: Vec<ResidualRow>,
    /// Missing rows.
    pub missing: Vec<MissingRow>,
    /// Coverage rows.
    pub coverage: Vec<CoverageRow>,
    /// Gap rows.
    pub gap: Vec<GapRow>,
    /// Symmetric-difference rows.
    #[serde(rename = "symmetricDifference")]
    pub symmetric_difference: Vec<SymmetricDifferenceRow>,
    /// Containment rows.
    pub containment: Vec<ContainmentRow>,
    /// Lead/lag rows.
    #[serde(rename = "leadLag")]
    pub lead_lag: Vec<LeadLagRow>,
    /// As-of rows.
    #[serde(rename = "asOf")]
    pub as_of: Vec<AsOfRow>,
}

/// Structured comparison result.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ComparisonResult {
    /// Result schema.
    pub schema: String,
    /// Schema version.
    #[serde(rename = "schemaVersion")]
    pub schema_version: u32,
    /// Artifact kind.
    pub artifact: String,
    /// Comparison plan.
    #[serde(skip)]
    pub plan: ComparisonPlan,
    /// Comparison plan name.
    #[serde(rename = "planName")]
    pub plan_name: String,
    /// Whether the result is valid.
    #[serde(rename = "isValid")]
    pub is_valid: bool,
    /// Validation and execution diagnostics.
    pub diagnostics: Vec<ComparisonDiagnostic>,
    /// Prepared artifact, when available.
    pub prepared: Option<Value>,
    /// Aligned artifact, when available.
    pub aligned: Option<Value>,
    /// Known-at point, when available.
    #[serde(rename = "knownAt")]
    pub known_at: Option<RowPoint>,
    /// Evaluation horizon, when available.
    #[serde(rename = "evaluationHorizon")]
    pub evaluation_horizon: Option<RowPoint>,
    /// Comparator summaries.
    #[serde(rename = "comparatorSummaries")]
    pub comparator_summaries: Vec<ComparatorSummary>,
    /// Coverage summaries.
    #[serde(rename = "coverageSummaries")]
    pub coverage_summaries: Vec<CoverageSummary>,
    /// Result rows grouped by family.
    pub rows: ComparisonRows,
    /// Overlap rows.
    #[serde(rename = "overlapRows")]
    pub overlap_rows: Vec<OverlapRow>,
    /// Residual rows.
    #[serde(rename = "residualRows")]
    pub residual_rows: Vec<ResidualRow>,
    /// Missing rows.
    #[serde(rename = "missingRows")]
    pub missing_rows: Vec<MissingRow>,
    /// Coverage rows.
    #[serde(rename = "coverageRows")]
    pub coverage_rows: Vec<CoverageRow>,
    /// Gap rows.
    #[serde(rename = "gapRows")]
    pub gap_rows: Vec<GapRow>,
    /// Symmetric-difference rows.
    #[serde(rename = "symmetricDifferenceRows")]
    pub symmetric_difference_rows: Vec<SymmetricDifferenceRow>,
    /// Containment rows.
    #[serde(rename = "containmentRows")]
    pub containment_rows: Vec<ContainmentRow>,
    /// Lead/lag rows.
    #[serde(rename = "leadLagRows")]
    pub lead_lag_rows: Vec<LeadLagRow>,
    /// Lead/lag summaries.
    #[serde(rename = "leadLagSummaries")]
    pub lead_lag_summaries: Vec<LeadLagSummary>,
    /// As-of rows.
    #[serde(rename = "asOfRows")]
    pub as_of_rows: Vec<AsOfRow>,
    /// Row finality metadata.
    #[serde(rename = "rowFinalities")]
    pub row_finalities: Vec<ComparisonRowFinality>,
    /// Serializable extension metadata.
    #[serde(rename = "extensionMetadata")]
    pub extension_metadata: Vec<ComparisonExtensionMetadata>,
}

#[derive(Clone, Debug)]
struct SegmentRef<'a> {
    start: i64,
    end: i64,
    record_id: &'a str,
    source: &'a str,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AlignedSegment {
    start: i64,
    end: i64,
    target_record_ids: Vec<String>,
    against_record_ids: Vec<String>,
    against_is_active: bool,
    against_active_sources: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TransitionPoint<'a> {
    record_id: &'a str,
    point: i64,
}

type GroupKey = (String, String, Option<String>);
type GroupWindows<'a> = (Vec<SegmentRef<'a>>, Vec<SegmentRef<'a>>);

struct ResultArtifacts {
    comparator_summaries: Vec<ComparatorSummary>,
    coverage_summaries: Vec<CoverageSummary>,
    lead_lag_summaries: Vec<LeadLagSummary>,
    row_finalities: Vec<ComparisonRowFinality>,
    extension_metadata: Vec<ComparisonExtensionMetadata>,
    rows: ComparisonRows,
}

/// Portable selected/excluded/normalized window artifact.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct WindowArtifact {
    /// Deterministic record ID.
    #[serde(rename = "recordId")]
    pub record_id: String,
    /// Window family.
    #[serde(rename = "windowName")]
    pub window_name: String,
    /// Logical key.
    pub key: String,
    /// Optional source.
    pub source: Option<String>,
    /// Optional partition.
    pub partition: Option<String>,
    /// Start processing position.
    #[serde(rename = "startPosition")]
    pub start_position: i64,
    /// End processing position, when closed or clipped.
    #[serde(rename = "endPosition")]
    pub end_position: Option<i64>,
    /// Known-at processing position, when supplied.
    #[serde(rename = "knownAtPosition")]
    pub known_at_position: Option<i64>,
    /// Whether the source window remained open.
    #[serde(rename = "isOpen")]
    pub is_open: bool,
    /// Segments.
    pub segments: Vec<WindowSegment>,
    /// Tags.
    pub tags: Vec<WindowTag>,
}

/// Excluded window artifact.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ExcludedWindowRecord {
    /// Deterministic record ID.
    #[serde(rename = "recordId")]
    pub record_id: String,
    /// Exclusion reason.
    pub reason: String,
    /// Diagnostic code, when any.
    #[serde(rename = "diagnosticCode")]
    pub diagnostic_code: Option<String>,
    /// Excluded window payload.
    pub window: WindowArtifact,
}

/// Normalized window artifact.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct NormalizedWindowRecord {
    /// Deterministic record ID.
    #[serde(rename = "recordId")]
    pub record_id: String,
    /// Selector name.
    #[serde(rename = "selectorName")]
    pub selector_name: String,
    /// Comparison side.
    pub side: ComparisonSide,
    /// Normalized range.
    pub range: RowRange,
    /// Whether the range depends on an open window clipped to a horizon.
    #[serde(rename = "isProvisional")]
    pub is_provisional: bool,
    /// Segments carried into alignment.
    pub segments: Vec<WindowSegment>,
    /// Backing window payload.
    pub window: WindowArtifact,
}

/// Prepared comparison artifact.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PreparedComparison {
    /// Source plan for the prepared comparison.
    #[serde(skip)]
    pub plan: ComparisonPlan,
    /// Preparation diagnostics.
    pub diagnostics: Vec<ComparisonDiagnostic>,
    /// Selected windows.
    #[serde(rename = "selectedWindows")]
    pub selected_windows: Vec<WindowArtifact>,
    /// Excluded windows.
    #[serde(rename = "excludedWindows")]
    pub excluded_windows: Vec<ExcludedWindowRecord>,
    /// Normalized windows.
    #[serde(rename = "normalizedWindows")]
    pub normalized_windows: Vec<NormalizedWindowRecord>,
}

/// Aligned segment artifact.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AlignedSegmentArtifact {
    /// Deterministic segment identifier.
    #[serde(rename = "segmentId")]
    pub segment_id: String,
    /// Window family.
    #[serde(rename = "windowName")]
    pub window_name: String,
    /// Logical key.
    pub key: String,
    /// Optional partition.
    pub partition: Option<String>,
    /// Aligned range.
    pub range: RowRange,
    /// Target record IDs covering the range.
    #[serde(rename = "targetRecordIds")]
    pub target_record_ids: Vec<String>,
    /// Comparison record IDs covering the range.
    #[serde(rename = "againstRecordIds")]
    pub against_record_ids: Vec<String>,
    /// Whether the comparison side was active after selector evaluation.
    #[serde(rename = "againstIsActive")]
    pub against_is_active: bool,
    /// Sources active on the comparison side during the aligned segment.
    #[serde(rename = "againstActiveSources")]
    pub against_active_sources: Vec<String>,
}

/// Aligned comparison artifact.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AlignedComparison {
    /// Source prepared comparison.
    #[serde(skip)]
    pub prepared: PreparedComparison,
    /// Deterministic aligned segments.
    pub segments: Vec<AlignedSegmentArtifact>,
}

/// Executes a comparison over closed windows.
#[must_use]
pub fn compare(history: &WindowHistory, plan: &ComparisonPlan) -> ComparisonResult {
    execute_compare(history, plan, None)
}

/// Executes a live comparison by clipping open windows to an evaluation horizon.
#[must_use]
pub fn compare_live(
    history: &WindowHistory,
    plan: &ComparisonPlan,
    evaluation_horizon: crate::TemporalPoint,
) -> ComparisonResult {
    execute_compare(history, plan, Some(evaluation_horizon))
}

/// Prepares a comparison without running comparators.
#[must_use]
pub fn prepare(history: &WindowHistory, plan: &ComparisonPlan) -> PreparedComparison {
    prepare_internal(history, plan, None)
}

/// Prepares a live comparison by clipping open windows to an evaluation horizon.
#[must_use]
pub fn prepare_live(
    history: &WindowHistory,
    plan: &ComparisonPlan,
    evaluation_horizon: crate::TemporalPoint,
) -> PreparedComparison {
    prepare_internal(history, plan, Some(evaluation_horizon))
}

/// Aligns prepared normalized windows into deterministic segments.
#[must_use]
pub fn align(prepared: &PreparedComparison) -> AlignedComparison {
    align_internal(prepared)
}

fn execute_compare(
    history: &WindowHistory,
    plan: &ComparisonPlan,
    live_horizon_override: Option<crate::TemporalPoint>,
) -> ComparisonResult {
    let mut diagnostics = Vec::new();
    if plan.strict && plan.scope_window.is_none() {
        diagnostics.push(ComparisonDiagnostic {
            code: "BroadSelector".to_owned(),
            severity: DiagnosticSeverity::Error,
        });
    }

    if diagnostics
        .iter()
        .any(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error)
    {
        return empty_result(&plan.name, diagnostics);
    }

    let prepared = prepare_internal(history, plan, live_horizon_override);
    diagnostics.extend(prepared.diagnostics.clone());
    let aligned = align_internal(&prepared);
    let groups = group_normalized_windows(&prepared);
    let mut rows = ComparisonRows::default();
    let mut comparator_summaries = Vec::new();
    let mut lead_lag_summaries = Vec::new();

    for comparator in &plan.comparators {
        let row_count = match comparator {
            Comparator::Overlap => {
                let emitted = build_overlap_rows(&groups, &plan.against);
                let count = emitted.len();
                rows.overlap.extend(emitted);
                count
            }
            Comparator::Residual => {
                let emitted = build_residual_rows(&groups, &plan.against);
                let count = emitted.len();
                rows.residual.extend(emitted);
                count
            }
            Comparator::Missing => {
                let emitted = build_missing_rows(&groups, &plan.against);
                let count = emitted.len();
                rows.missing.extend(emitted);
                count
            }
            Comparator::Coverage => {
                let emitted = build_coverage_rows(&groups, &plan.against);
                let count = emitted.len();
                rows.coverage.extend(emitted);
                count
            }
            Comparator::Gap => {
                let emitted = build_gap_rows(&groups, &plan.against);
                let count = emitted.len();
                rows.gap.extend(emitted);
                count
            }
            Comparator::SymmetricDifference => {
                let emitted = build_symmetric_difference_rows(&groups, &plan.against);
                let count = emitted.len();
                rows.symmetric_difference.extend(emitted);
                count
            }
            Comparator::Containment => {
                let emitted = build_containment_rows(&groups, &plan.against);
                let count = emitted.len();
                rows.containment.extend(emitted);
                count
            }
            Comparator::LeadLag {
                transition,
                axis,
                tolerance_magnitude,
            } => {
                let (emitted, summary) =
                    build_lead_lag_rows(&groups, transition.clone(), *axis, *tolerance_magnitude);
                let count = emitted.len();
                rows.lead_lag.extend(emitted);
                lead_lag_summaries.push(summary);
                count
            }
            Comparator::AsOf {
                direction,
                axis,
                tolerance_magnitude,
            } => {
                let (emitted, extra_diagnostics) =
                    build_as_of_rows(&groups, direction.clone(), *axis, *tolerance_magnitude);
                let count = emitted.len();
                rows.as_of.extend(emitted);
                diagnostics.extend(extra_diagnostics);
                count
            }
        };

        comparator_summaries.push(ComparatorSummary {
            comparator_name: comparator.declaration(),
            row_count,
        });
    }

    let provisional_record_ids = prepared
        .normalized_windows
        .iter()
        .filter(|window| window.is_provisional)
        .map(|window| window.record_id.clone())
        .collect::<BTreeSet<_>>();

    let mut result = materialize_result(
        plan,
        &plan.name,
        !diagnostics
            .iter()
            .any(|diagnostic| diagnostic.severity == DiagnosticSeverity::Error),
        diagnostics,
        ResultArtifacts {
            comparator_summaries,
            coverage_summaries: build_coverage_summaries(&rows.coverage),
            lead_lag_summaries,
            row_finalities: build_row_finalities(&rows, &provisional_record_ids),
            extension_metadata: build_extension_metadata(&aligned, plan),
            rows,
        },
    );
    result.known_at = plan.known_at.map(row_point_from_temporal_point);
    result.evaluation_horizon = live_horizon_override
        .or(plan.open_window_horizon)
        .map(row_point_from_temporal_point);
    result.prepared = Some(serde_json::to_value(prepared).expect("prepared artifact"));
    result.aligned = Some(serde_json::to_value(aligned).expect("aligned artifact"));
    result
}

fn parse_parameterized_comparator(value: &str) -> Option<Comparator> {
    let parts: Vec<&str> = value.split(':').collect();
    match parts.as_slice() {
        ["lead-lag", transition, axis, tolerance] => Some(Comparator::LeadLag {
            transition: parse_lead_lag_transition(transition)?,
            axis: parse_temporal_axis(axis)?,
            tolerance_magnitude: parse_non_negative_i64(tolerance)?,
        }),
        ["asof", direction, axis, tolerance] => Some(Comparator::AsOf {
            direction: parse_as_of_direction(direction)?,
            axis: parse_temporal_axis(axis)?,
            tolerance_magnitude: parse_non_negative_i64(tolerance)?,
        }),
        _ => None,
    }
}

fn parse_temporal_axis(value: &str) -> Option<TemporalAxis> {
    match value {
        "ProcessingPosition" => Some(TemporalAxis::ProcessingPosition),
        "Timestamp" => Some(TemporalAxis::Timestamp),
        _ => None,
    }
}

fn parse_lead_lag_transition(value: &str) -> Option<LeadLagTransition> {
    match value {
        "Start" => Some(LeadLagTransition::Start),
        "End" => Some(LeadLagTransition::End),
        _ => None,
    }
}

fn parse_as_of_direction(value: &str) -> Option<AsOfDirection> {
    match value {
        "Previous" => Some(AsOfDirection::Previous),
        "Next" => Some(AsOfDirection::Next),
        "Nearest" => Some(AsOfDirection::Nearest),
        _ => None,
    }
}

fn parse_non_negative_i64(value: &str) -> Option<i64> {
    let parsed = value.parse::<i64>().ok()?;
    (parsed >= 0).then_some(parsed)
}

fn empty_result(plan_name: &str, diagnostics: Vec<ComparisonDiagnostic>) -> ComparisonResult {
    materialize_result(
        &ComparisonPlan {
            name: plan_name.to_owned(),
            target_source: String::new(),
            against: AgainstSelection::Sources(Vec::new()),
            scope_window: None,
            scope_segments: Vec::new(),
            scope_tags: Vec::new(),
            comparators: Vec::new(),
            known_at: None,
            open_window_policy: OpenWindowPolicy::RequireClosed,
            open_window_horizon: None,
            strict: false,
        },
        plan_name,
        false,
        diagnostics,
        ResultArtifacts {
            comparator_summaries: Vec::new(),
            coverage_summaries: Vec::new(),
            lead_lag_summaries: Vec::new(),
            row_finalities: Vec::new(),
            extension_metadata: Vec::new(),
            rows: ComparisonRows::default(),
        },
    )
}

fn materialize_result(
    plan: &ComparisonPlan,
    plan_name: &str,
    is_valid: bool,
    diagnostics: Vec<ComparisonDiagnostic>,
    artifacts: ResultArtifacts,
) -> ComparisonResult {
    ComparisonResult {
        schema: "spanfold.comparison.result".to_owned(),
        schema_version: 0,
        artifact: "result".to_owned(),
        plan: plan.clone(),
        plan_name: plan_name.to_owned(),
        is_valid,
        diagnostics,
        prepared: None,
        aligned: None,
        known_at: None,
        evaluation_horizon: None,
        comparator_summaries: artifacts.comparator_summaries,
        coverage_summaries: artifacts.coverage_summaries,
        overlap_rows: artifacts.rows.overlap.clone(),
        residual_rows: artifacts.rows.residual.clone(),
        missing_rows: artifacts.rows.missing.clone(),
        coverage_rows: artifacts.rows.coverage.clone(),
        gap_rows: artifacts.rows.gap.clone(),
        symmetric_difference_rows: artifacts.rows.symmetric_difference.clone(),
        containment_rows: artifacts.rows.containment.clone(),
        lead_lag_rows: artifacts.rows.lead_lag.clone(),
        lead_lag_summaries: artifacts.lead_lag_summaries,
        as_of_rows: artifacts.rows.as_of.clone(),
        row_finalities: artifacts.row_finalities,
        extension_metadata: artifacts.extension_metadata,
        rows: artifacts.rows,
    }
}

fn build_coverage_summaries(rows: &[CoverageRow]) -> Vec<CoverageSummary> {
    let mut grouped: BTreeMap<(String, String, Option<String>), (f64, f64)> = BTreeMap::new();
    for row in rows {
        let entry = grouped
            .entry((
                row.window_name.clone(),
                row.key.clone(),
                row.partition.clone(),
            ))
            .or_insert((0.0, 0.0));
        entry.0 += row.target_magnitude as f64;
        entry.1 += row.covered_magnitude as f64;
    }

    grouped
        .into_iter()
        .map(
            |((window_name, key, partition), (target_magnitude, covered_magnitude))| {
                CoverageSummary {
                    window_name,
                    key,
                    partition,
                    target_magnitude,
                    covered_magnitude,
                    coverage_ratio: if target_magnitude == 0.0 {
                        0.0
                    } else {
                        covered_magnitude / target_magnitude
                    },
                }
            },
        )
        .collect()
}

fn build_extension_metadata(
    aligned: &AlignedComparison,
    plan: &ComparisonPlan,
) -> Vec<ComparisonExtensionMetadata> {
    let AgainstSelection::Cohort {
        activity, sources, ..
    } = &plan.against
    else {
        return Vec::new();
    };

    aligned
        .segments
        .iter()
        .enumerate()
        .map(|(index, segment)| ComparisonExtensionMetadata {
            extension_id: "spanfold.cohort".to_owned(),
            key: format!("segment[{index}]"),
            value: format!(
                "rule={}; required={}; activeCount={}; isActive={}; activeSources={}",
                activity.name(),
                required_activity_count(activity, sources.len()),
                segment.against_active_sources.len(),
                segment.against_is_active,
                segment.against_active_sources.join(",")
            ),
        })
        .collect()
}

fn required_activity_count(activity: &CohortActivity, member_count: usize) -> usize {
    match activity {
        CohortActivity::Any => 1,
        CohortActivity::All => member_count,
        CohortActivity::None => 0,
        CohortActivity::AtLeast { count }
        | CohortActivity::AtMost { count }
        | CohortActivity::Exactly { count } => *count,
    }
}

fn build_row_finalities(
    rows: &ComparisonRows,
    provisional_record_ids: &BTreeSet<String>,
) -> Vec<ComparisonRowFinality> {
    let mut finalities = Vec::new();
    append_overlap_finalities(&mut finalities, &rows.overlap, provisional_record_ids);
    append_residual_finalities(&mut finalities, &rows.residual, provisional_record_ids);
    append_missing_finalities(&mut finalities, &rows.missing, provisional_record_ids);
    append_coverage_finalities(&mut finalities, &rows.coverage, provisional_record_ids);
    append_finalities(&mut finalities, "gap", rows.gap.len());
    append_symmetric_difference_finalities(
        &mut finalities,
        &rows.symmetric_difference,
        provisional_record_ids,
    );
    append_containment_finalities(&mut finalities, &rows.containment, provisional_record_ids);
    append_lead_lag_finalities(&mut finalities, &rows.lead_lag, provisional_record_ids);
    append_as_of_finalities(&mut finalities, &rows.as_of, provisional_record_ids);
    finalities
}

fn append_finalities(finalities: &mut Vec<ComparisonRowFinality>, row_type: &str, count: usize) {
    for index in 0..count {
        finalities.push(ComparisonRowFinality {
            row_type: row_type.to_owned(),
            row_id: format!("{row_type}[{index}]"),
            finality: ComparisonFinality::Final,
            reason: "derived from closed windows".to_owned(),
            version: 1,
            supersedes_row_id: None,
        });
    }
}

fn append_overlap_finalities(
    finalities: &mut Vec<ComparisonRowFinality>,
    rows: &[OverlapRow],
    provisional_record_ids: &BTreeSet<String>,
) {
    for (index, row) in rows.iter().enumerate() {
        push_finality(
            finalities,
            "overlap",
            index,
            row.target_record_ids
                .iter()
                .chain(row.against_record_ids.iter())
                .any(|id| provisional_record_ids.contains(id)),
        );
    }
}

fn append_residual_finalities(
    finalities: &mut Vec<ComparisonRowFinality>,
    rows: &[ResidualRow],
    provisional_record_ids: &BTreeSet<String>,
) {
    for (index, row) in rows.iter().enumerate() {
        push_finality(
            finalities,
            "residual",
            index,
            row.target_record_ids
                .iter()
                .any(|id| provisional_record_ids.contains(id)),
        );
    }
}

fn append_missing_finalities(
    finalities: &mut Vec<ComparisonRowFinality>,
    rows: &[MissingRow],
    provisional_record_ids: &BTreeSet<String>,
) {
    for (index, row) in rows.iter().enumerate() {
        push_finality(
            finalities,
            "missing",
            index,
            row.against_record_ids
                .iter()
                .any(|id| provisional_record_ids.contains(id)),
        );
    }
}

fn append_coverage_finalities(
    finalities: &mut Vec<ComparisonRowFinality>,
    rows: &[CoverageRow],
    provisional_record_ids: &BTreeSet<String>,
) {
    for (index, row) in rows.iter().enumerate() {
        push_finality(
            finalities,
            "coverage",
            index,
            row.target_record_ids
                .iter()
                .chain(row.against_record_ids.iter())
                .any(|id| provisional_record_ids.contains(id)),
        );
    }
}

fn append_symmetric_difference_finalities(
    finalities: &mut Vec<ComparisonRowFinality>,
    rows: &[SymmetricDifferenceRow],
    provisional_record_ids: &BTreeSet<String>,
) {
    for (index, row) in rows.iter().enumerate() {
        push_finality(
            finalities,
            "symmetricDifference",
            index,
            row.target_record_ids
                .iter()
                .chain(row.against_record_ids.iter())
                .any(|id| provisional_record_ids.contains(id)),
        );
    }
}

fn append_containment_finalities(
    finalities: &mut Vec<ComparisonRowFinality>,
    rows: &[ContainmentRow],
    provisional_record_ids: &BTreeSet<String>,
) {
    for (index, row) in rows.iter().enumerate() {
        push_finality(
            finalities,
            "containment",
            index,
            row.target_record_ids
                .iter()
                .chain(row.container_record_ids.iter())
                .any(|id| provisional_record_ids.contains(id)),
        );
    }
}

fn append_lead_lag_finalities(
    finalities: &mut Vec<ComparisonRowFinality>,
    rows: &[LeadLagRow],
    provisional_record_ids: &BTreeSet<String>,
) {
    for (index, row) in rows.iter().enumerate() {
        push_finality(
            finalities,
            "leadLag",
            index,
            provisional_record_ids.contains(&row.target_record_id)
                || row
                    .comparison_record_id
                    .as_ref()
                    .is_some_and(|id| provisional_record_ids.contains(id)),
        );
    }
}

fn append_as_of_finalities(
    finalities: &mut Vec<ComparisonRowFinality>,
    rows: &[AsOfRow],
    provisional_record_ids: &BTreeSet<String>,
) {
    for (index, row) in rows.iter().enumerate() {
        push_finality(
            finalities,
            "asOf",
            index,
            provisional_record_ids.contains(&row.target_record_id)
                || row
                    .matched_record_id
                    .as_ref()
                    .is_some_and(|id| provisional_record_ids.contains(id)),
        );
    }
}

fn push_finality(
    finalities: &mut Vec<ComparisonRowFinality>,
    row_type: &str,
    index: usize,
    provisional: bool,
) {
    finalities.push(ComparisonRowFinality {
        row_type: row_type.to_owned(),
        row_id: format!("{row_type}[{index}]"),
        finality: if provisional {
            ComparisonFinality::Provisional
        } else {
            ComparisonFinality::Final
        },
        reason: if provisional {
            "depends on an open window clipped to the evaluation horizon".to_owned()
        } else {
            "derived from closed windows".to_owned()
        },
        version: 1,
        supersedes_row_id: None,
    });
}

fn prepare_internal(
    history: &WindowHistory,
    plan: &ComparisonPlan,
    live_horizon_override: Option<crate::TemporalPoint>,
) -> PreparedComparison {
    let mut diagnostics = Vec::new();
    let mut selected_windows = Vec::new();
    let mut excluded_windows = Vec::new();
    let mut normalized_windows = Vec::new();

    let mut candidates = history
        .closed_windows()
        .iter()
        .map(RawWindowRef::Closed)
        .collect::<Vec<_>>();
    candidates.extend(history.open_windows().iter().map(RawWindowRef::Open));

    candidates.sort_by(|left, right| {
        (
            left.window_name(),
            left.key(),
            left.source().unwrap_or(""),
            left.partition().unwrap_or(""),
            left.start_position(),
            left.end_position().unwrap_or(i64::MAX),
            left.record_id(),
        )
            .cmp(&(
                right.window_name(),
                right.key(),
                right.source().unwrap_or(""),
                right.partition().unwrap_or(""),
                right.start_position(),
                right.end_position().unwrap_or(i64::MAX),
                right.record_id(),
            ))
    });

    for candidate in candidates {
        let window = to_window_artifact(&candidate);
        let known_at_position = candidate
            .known_at_position()
            .unwrap_or(candidate.start_position());
        if let Some(known_at) = plan.known_at
            && (known_at.axis() != TemporalAxis::ProcessingPosition
                || known_at_position > known_at.magnitude())
        {
            excluded_windows.push(ExcludedWindowRecord {
                record_id: window.record_id.clone(),
                reason: "Window was not available at the configured known-at point.".to_owned(),
                diagnostic_code: Some("FutureWindowExcluded".to_owned()),
                window,
            });
            diagnostics.push(ComparisonDiagnostic {
                code: "FutureWindowExcluded".to_owned(),
                severity: DiagnosticSeverity::Warning,
            });
            continue;
        }

        if let Some(scope_window) = &plan.scope_window
            && candidate.window_name() != scope_window
        {
            excluded_windows.push(ExcludedWindowRecord {
                record_id: window.record_id.clone(),
                reason: "Window is outside the comparison scope.".to_owned(),
                diagnostic_code: None,
                window,
            });
            continue;
        }

        if !matches_window_artifact(&window, &plan.scope_segments, &plan.scope_tags) {
            excluded_windows.push(ExcludedWindowRecord {
                record_id: window.record_id.clone(),
                reason: "Window is outside the comparison scope.".to_owned(),
                diagnostic_code: None,
                window,
            });
            continue;
        }

        let is_target = candidate.source() == Some(plan.target_source.as_str());
        let is_against = match &plan.against {
            AgainstSelection::Sources(sources) => candidate
                .source()
                .is_some_and(|source| sources.iter().any(|item| item == source)),
            AgainstSelection::Cohort { sources, .. } => candidate
                .source()
                .is_some_and(|source| sources.iter().any(|item| item == source)),
        };
        if !is_target && !is_against {
            excluded_windows.push(ExcludedWindowRecord {
                record_id: window.record_id.clone(),
                reason: "Window did not match target or comparison selectors.".to_owned(),
                diagnostic_code: None,
                window,
            });
            continue;
        }

        selected_windows.push(window.clone());
        if is_target
            && let Some(normalized) = normalize_candidate(
                &candidate,
                "target",
                ComparisonSide::Target,
                plan,
                live_horizon_override,
                &mut diagnostics,
                &mut excluded_windows,
            )
        {
            normalized_windows.push(normalized);
        }
        if is_against
            && let Some(normalized) = normalize_candidate(
                &candidate,
                "against",
                ComparisonSide::Against,
                plan,
                live_horizon_override,
                &mut diagnostics,
                &mut excluded_windows,
            )
        {
            normalized_windows.push(normalized);
        }
    }

    PreparedComparison {
        plan: plan.clone(),
        diagnostics,
        selected_windows,
        excluded_windows,
        normalized_windows,
    }
}

fn align_internal(prepared: &PreparedComparison) -> AlignedComparison {
    let groups = group_normalized_windows(prepared);
    let mut segments = Vec::new();
    for ((window_name, key, partition), (targets, againsts)) in groups {
        for segment in aligned_segments(
            targets.as_slice(),
            againsts.as_slice(),
            &prepared.plan.against,
        )
        .into_iter()
        .filter(|segment| {
            !segment.target_record_ids.is_empty() || !segment.against_record_ids.is_empty()
        }) {
            segments.push(AlignedSegmentArtifact {
                segment_id: format!("segment[{}]", segments.len()),
                window_name: window_name.clone(),
                key: key.clone(),
                partition: partition.clone(),
                range: RowRange {
                    start: segment.start,
                    end: segment.end,
                },
                target_record_ids: segment.target_record_ids,
                against_record_ids: segment.against_record_ids,
                against_is_active: segment.against_is_active,
                against_active_sources: segment.against_active_sources,
            });
        }
    }
    AlignedComparison {
        prepared: prepared.clone(),
        segments,
    }
}

fn group_normalized_windows(prepared: &PreparedComparison) -> BTreeMap<GroupKey, GroupWindows<'_>> {
    let mut groups: BTreeMap<GroupKey, GroupWindows<'_>> = BTreeMap::new();
    for normalized in &prepared.normalized_windows {
        let group = groups
            .entry((
                normalized.window.window_name.clone(),
                normalized.window.key.clone(),
                normalized.window.partition.clone(),
            ))
            .or_default();
        let segment = SegmentRef {
            start: normalized.range.start,
            end: normalized.range.end,
            record_id: normalized.record_id.as_str(),
            source: normalized.window.source.as_deref().unwrap_or(""),
        };
        match normalized.side {
            ComparisonSide::Target => group.0.push(segment),
            ComparisonSide::Against => group.1.push(segment),
        }
    }
    groups
}

fn row_point_from_temporal_point(point: crate::TemporalPoint) -> RowPoint {
    RowPoint {
        axis: point.axis(),
        magnitude: point.magnitude(),
    }
}

enum RawWindowRef<'a> {
    Closed(&'a crate::ClosedWindow),
    Open(&'a crate::OpenWindow),
}

impl RawWindowRef<'_> {
    fn record_id(&self) -> &str {
        match self {
            Self::Closed(window) => window.id.as_str(),
            Self::Open(window) => window.id.as_str(),
        }
    }

    fn window_name(&self) -> &str {
        match self {
            Self::Closed(window) => &window.window_name,
            Self::Open(window) => &window.window_name,
        }
    }

    fn key(&self) -> &str {
        match self {
            Self::Closed(window) => &window.key,
            Self::Open(window) => &window.key,
        }
    }

    fn source(&self) -> Option<&str> {
        match self {
            Self::Closed(window) => window.source.as_deref(),
            Self::Open(window) => window.source.as_deref(),
        }
    }

    fn partition(&self) -> Option<&str> {
        match self {
            Self::Closed(window) => window.partition.as_deref(),
            Self::Open(window) => window.partition.as_deref(),
        }
    }

    fn start_position(&self) -> i64 {
        match self {
            Self::Closed(window) => window.range.start().magnitude(),
            Self::Open(window) => window.start.magnitude(),
        }
    }

    fn end_position(&self) -> Option<i64> {
        match self {
            Self::Closed(window) => Some(window.range.end().magnitude()),
            Self::Open(_) => None,
        }
    }

    fn known_at_position(&self) -> Option<i64> {
        match self {
            Self::Closed(window) => window.known_at.map(|point| point.magnitude()),
            Self::Open(window) => window.known_at.map(|point| point.magnitude()),
        }
    }

    fn segments(&self) -> &[WindowSegment] {
        match self {
            Self::Closed(window) => &window.segments,
            Self::Open(window) => &window.segments,
        }
    }

    fn tags(&self) -> &[WindowTag] {
        match self {
            Self::Closed(window) => &window.tags,
            Self::Open(window) => &window.tags,
        }
    }

    fn is_open(&self) -> bool {
        matches!(self, Self::Open(_))
    }
}

fn to_window_artifact(candidate: &RawWindowRef<'_>) -> WindowArtifact {
    WindowArtifact {
        record_id: candidate.record_id().to_owned(),
        window_name: candidate.window_name().to_owned(),
        key: candidate.key().to_owned(),
        source: candidate.source().map(str::to_owned),
        partition: candidate.partition().map(str::to_owned),
        start_position: candidate.start_position(),
        end_position: candidate.end_position(),
        known_at_position: candidate.known_at_position(),
        is_open: candidate.is_open(),
        segments: candidate.segments().to_vec(),
        tags: candidate.tags().to_vec(),
    }
}

fn normalize_candidate(
    candidate: &RawWindowRef<'_>,
    selector_name: &str,
    side: ComparisonSide,
    plan: &ComparisonPlan,
    live_horizon_override: Option<crate::TemporalPoint>,
    diagnostics: &mut Vec<ComparisonDiagnostic>,
    excluded_windows: &mut Vec<ExcludedWindowRecord>,
) -> Option<NormalizedWindowRecord> {
    let horizon = live_horizon_override.or(plan.open_window_horizon);
    let end_position = match candidate.end_position() {
        Some(end) => (end, false),
        None => match (plan.open_window_policy, horizon) {
            (OpenWindowPolicy::ClipToHorizon, Some(point))
                if point.axis() == TemporalAxis::ProcessingPosition
                    && point.magnitude() >= candidate.start_position() =>
            {
                (point.magnitude(), true)
            }
            (OpenWindowPolicy::ClipToHorizon, Some(_)) => {
                let window = to_window_artifact(candidate);
                excluded_windows.push(ExcludedWindowRecord {
                    record_id: window.record_id.clone(),
                    reason: "Open-window horizon cannot be earlier than the window start."
                        .to_owned(),
                    diagnostic_code: Some("InvalidRangeDuration".to_owned()),
                    window,
                });
                diagnostics.push(ComparisonDiagnostic {
                    code: "InvalidRangeDuration".to_owned(),
                    severity: DiagnosticSeverity::Error,
                });
                return None;
            }
            _ => {
                let window = to_window_artifact(candidate);
                excluded_windows.push(ExcludedWindowRecord {
                    record_id: window.record_id.clone(),
                    reason: "Open windows require an explicit clipping policy.".to_owned(),
                    diagnostic_code: Some("OpenWindowsWithoutPolicy".to_owned()),
                    window,
                });
                diagnostics.push(ComparisonDiagnostic {
                    code: "OpenWindowsWithoutPolicy".to_owned(),
                    severity: DiagnosticSeverity::Error,
                });
                return None;
            }
        },
    };

    Some(NormalizedWindowRecord {
        record_id: candidate.record_id().to_owned(),
        selector_name: selector_name.to_owned(),
        side,
        range: RowRange {
            start: candidate.start_position(),
            end: end_position.0,
        },
        is_provisional: end_position.1,
        segments: candidate.segments().to_vec(),
        window: to_window_artifact(candidate),
    })
}

fn matches_window_artifact(
    window: &WindowArtifact,
    segment_filters: &[WindowFilter],
    tag_filters: &[WindowFilter],
) -> bool {
    segment_filters.iter().all(|filter| {
        window
            .segments
            .iter()
            .any(|item| item.name == filter.name && item.value == filter.value)
    }) && tag_filters.iter().all(|filter| {
        window
            .tags
            .iter()
            .any(|item| item.name == filter.name && item.value == filter.value)
    })
}

fn aligned_segments(
    targets: &[SegmentRef<'_>],
    againsts: &[SegmentRef<'_>],
    against_selection: &AgainstSelection,
) -> Vec<AlignedSegment> {
    let mut points = BTreeSet::new();
    for item in targets {
        points.insert(item.start);
        points.insert(item.end);
    }
    for item in againsts {
        points.insert(item.start);
        points.insert(item.end);
    }

    let points: Vec<i64> = points.into_iter().collect();
    let mut segments = Vec::new();
    for pair in points.windows(2) {
        let start = pair[0];
        let end = pair[1];
        if start >= end {
            continue;
        }

        let mut target_record_ids = Vec::new();
        let mut against_record_ids = Vec::new();
        for item in targets {
            if item.start < end && item.end > start {
                target_record_ids.push(item.record_id.to_owned());
            }
        }
        for item in againsts {
            if item.start < end && item.end > start {
                against_record_ids.push(item.record_id.to_owned());
            }
        }

        let active_sources = againsts
            .iter()
            .filter(|item| item.start < end && item.end > start)
            .map(|item| item.source)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();

        let against_is_active = match against_selection {
            AgainstSelection::Sources(_) => !active_sources.is_empty(),
            AgainstSelection::Cohort {
                sources, activity, ..
            } => activity.is_active(active_sources.len(), sources.len()),
        };

        segments.push(AlignedSegment {
            start,
            end,
            target_record_ids,
            against_record_ids,
            against_is_active,
            against_active_sources: active_sources,
        });
    }
    segments
}

fn build_overlap_rows(
    groups: &BTreeMap<GroupKey, GroupWindows<'_>>,
    against_selection: &AgainstSelection,
) -> Vec<OverlapRow> {
    let mut rows = Vec::new();
    for ((window_name, key, partition), (targets, againsts)) in groups {
        for segment in aligned_segments(targets, againsts, against_selection) {
            if segment.target_record_ids.is_empty() || !segment.against_is_active {
                continue;
            }

            rows.push(OverlapRow {
                window_name: window_name.clone(),
                key: key.clone(),
                partition: partition.clone(),
                range: RowRange {
                    start: segment.start,
                    end: segment.end,
                },
                target_record_ids: segment.target_record_ids,
                against_record_ids: segment.against_record_ids,
            });
        }
    }
    rows
}

fn build_residual_rows(
    groups: &BTreeMap<GroupKey, GroupWindows<'_>>,
    against_selection: &AgainstSelection,
) -> Vec<ResidualRow> {
    let mut rows = Vec::new();
    for ((window_name, key, partition), (targets, againsts)) in groups {
        for segment in aligned_segments(targets, againsts, against_selection) {
            if segment.target_record_ids.is_empty() || segment.against_is_active {
                continue;
            }

            rows.push(ResidualRow {
                window_name: window_name.clone(),
                key: key.clone(),
                partition: partition.clone(),
                range: RowRange {
                    start: segment.start,
                    end: segment.end,
                },
                target_record_ids: segment.target_record_ids,
            });
        }
    }
    rows
}

fn build_missing_rows(
    groups: &BTreeMap<GroupKey, GroupWindows<'_>>,
    against_selection: &AgainstSelection,
) -> Vec<MissingRow> {
    let mut rows = Vec::new();
    for ((window_name, key, partition), (targets, againsts)) in groups {
        for segment in aligned_segments(targets, againsts, against_selection) {
            if !segment.target_record_ids.is_empty() || !segment.against_is_active {
                continue;
            }

            rows.push(MissingRow {
                window_name: window_name.clone(),
                key: key.clone(),
                partition: partition.clone(),
                range: RowRange {
                    start: segment.start,
                    end: segment.end,
                },
                against_record_ids: segment.against_record_ids,
            });
        }
    }
    rows
}

fn build_coverage_rows(
    groups: &BTreeMap<GroupKey, GroupWindows<'_>>,
    against_selection: &AgainstSelection,
) -> Vec<CoverageRow> {
    let mut rows = Vec::new();
    for ((window_name, key, partition), (targets, againsts)) in groups {
        for segment in aligned_segments(targets, againsts, against_selection) {
            if segment.target_record_ids.is_empty() {
                continue;
            }

            rows.push(CoverageRow {
                window_name: window_name.clone(),
                key: key.clone(),
                partition: partition.clone(),
                range: RowRange {
                    start: segment.start,
                    end: segment.end,
                },
                target_magnitude: segment.end - segment.start,
                covered_magnitude: if segment.against_is_active {
                    segment.end - segment.start
                } else {
                    0
                },
                target_record_ids: segment.target_record_ids,
                against_record_ids: segment.against_record_ids,
            });
        }
    }
    rows
}

fn build_gap_rows(
    groups: &BTreeMap<GroupKey, GroupWindows<'_>>,
    against_selection: &AgainstSelection,
) -> Vec<GapRow> {
    let mut rows = Vec::new();
    for ((window_name, key, partition), (targets, againsts)) in groups {
        for segment in aligned_segments(targets, againsts, against_selection) {
            if !segment.target_record_ids.is_empty() || segment.against_is_active {
                continue;
            }

            rows.push(GapRow {
                window_name: window_name.clone(),
                key: key.clone(),
                partition: partition.clone(),
                range: RowRange {
                    start: segment.start,
                    end: segment.end,
                },
            });
        }
    }
    rows
}

fn build_symmetric_difference_rows(
    groups: &BTreeMap<GroupKey, GroupWindows<'_>>,
    against_selection: &AgainstSelection,
) -> Vec<SymmetricDifferenceRow> {
    let mut rows = Vec::new();
    for ((window_name, key, partition), (targets, againsts)) in groups {
        for segment in aligned_segments(targets, againsts, against_selection) {
            let has_target = !segment.target_record_ids.is_empty();
            let has_against = segment.against_is_active;
            if has_target == has_against {
                continue;
            }

            rows.push(SymmetricDifferenceRow {
                window_name: window_name.clone(),
                key: key.clone(),
                partition: partition.clone(),
                range: RowRange {
                    start: segment.start,
                    end: segment.end,
                },
                side: if has_target {
                    ComparisonSide::Target
                } else {
                    ComparisonSide::Against
                },
                target_record_ids: segment.target_record_ids,
                against_record_ids: segment.against_record_ids,
            });
        }
    }
    rows
}

fn build_containment_rows(
    groups: &BTreeMap<GroupKey, GroupWindows<'_>>,
    against_selection: &AgainstSelection,
) -> Vec<ContainmentRow> {
    let mut rows = Vec::new();
    for ((window_name, key, partition), (targets, againsts)) in groups {
        let mut target_ranges = BTreeMap::new();
        for target in targets {
            target_ranges.insert(target.record_id, (target.start, target.end));
        }

        for segment in aligned_segments(targets, againsts, against_selection) {
            if segment.target_record_ids.is_empty() {
                continue;
            }

            if segment.against_is_active {
                rows.push(ContainmentRow {
                    window_name: window_name.clone(),
                    key: key.clone(),
                    partition: partition.clone(),
                    range: RowRange {
                        start: segment.start,
                        end: segment.end,
                    },
                    status: ContainmentStatus::Contained,
                    target_record_ids: segment.target_record_ids,
                    container_record_ids: segment.against_record_ids,
                });
                continue;
            }

            for target_record_id in segment.target_record_ids {
                rows.push(ContainmentRow {
                    window_name: window_name.clone(),
                    key: key.clone(),
                    partition: partition.clone(),
                    range: RowRange {
                        start: segment.start,
                        end: segment.end,
                    },
                    status: classify_uncontained_segment(
                        target_ranges.get(target_record_id.as_str()),
                        (segment.start, segment.end),
                    ),
                    target_record_ids: vec![target_record_id],
                    container_record_ids: Vec::new(),
                });
            }
        }
    }
    rows
}

fn classify_uncontained_segment(
    target_range: Option<&(i64, i64)>,
    segment_range: (i64, i64),
) -> ContainmentStatus {
    let Some(&(target_start, target_end)) = target_range else {
        return ContainmentStatus::NotContained;
    };

    if segment_range.0 == target_start {
        return ContainmentStatus::LeftOverhang;
    }
    if segment_range.1 == target_end {
        return ContainmentStatus::RightOverhang;
    }
    ContainmentStatus::NotContained
}

fn build_lead_lag_rows(
    groups: &BTreeMap<GroupKey, GroupWindows<'_>>,
    transition: LeadLagTransition,
    axis: TemporalAxis,
    tolerance_magnitude: i64,
) -> (Vec<LeadLagRow>, LeadLagSummary) {
    if axis != TemporalAxis::ProcessingPosition {
        return (
            Vec::new(),
            LeadLagSummary {
                transition,
                axis,
                tolerance_magnitude,
                row_count: 0,
                target_lead_count: 0,
                target_lag_count: 0,
                equal_count: 0,
                missing_comparison_count: 0,
                outside_tolerance_count: 0,
                minimum_delta_magnitude: None,
                maximum_delta_magnitude: None,
            },
        );
    }

    let mut rows = Vec::new();
    for ((window_name, key, partition), (targets, againsts)) in groups {
        let mut comparison_points: Vec<TransitionPoint<'_>> = againsts
            .iter()
            .map(|against| TransitionPoint {
                record_id: against.record_id,
                point: if transition == LeadLagTransition::Start {
                    against.start
                } else {
                    against.end
                },
            })
            .collect();
        comparison_points.sort_by_key(|item| (item.point, item.record_id));

        for target in targets {
            let target_point = if transition == LeadLagTransition::Start {
                target.start
            } else {
                target.end
            };

            if comparison_points.is_empty() {
                rows.push(LeadLagRow {
                    window_name: window_name.clone(),
                    key: key.clone(),
                    partition: partition.clone(),
                    transition: transition.clone(),
                    axis,
                    target_point: RowPoint {
                        axis,
                        magnitude: target_point,
                    },
                    comparison_point: None,
                    delta_magnitude: None,
                    tolerance_magnitude,
                    is_within_tolerance: false,
                    direction: LeadLagDirection::MissingComparison,
                    target_record_id: target.record_id.to_owned(),
                    comparison_record_id: None,
                });
                continue;
            }

            let nearest = find_nearest_transition(&comparison_points, target_point);
            let delta = target_point - nearest.point;
            rows.push(LeadLagRow {
                window_name: window_name.clone(),
                key: key.clone(),
                partition: partition.clone(),
                transition: transition.clone(),
                axis,
                target_point: RowPoint {
                    axis,
                    magnitude: target_point,
                },
                comparison_point: Some(RowPoint {
                    axis,
                    magnitude: nearest.point,
                }),
                delta_magnitude: Some(delta),
                tolerance_magnitude,
                is_within_tolerance: delta.abs() <= tolerance_magnitude,
                direction: direction_for_delta(delta),
                target_record_id: target.record_id.to_owned(),
                comparison_record_id: Some(nearest.record_id.to_owned()),
            });
        }
    }

    let mut summary = LeadLagSummary {
        transition,
        axis,
        tolerance_magnitude,
        row_count: rows.len(),
        target_lead_count: 0,
        target_lag_count: 0,
        equal_count: 0,
        missing_comparison_count: 0,
        outside_tolerance_count: 0,
        minimum_delta_magnitude: None,
        maximum_delta_magnitude: None,
    };
    for row in &rows {
        if !row.is_within_tolerance {
            summary.outside_tolerance_count += 1;
        }
        match row.direction {
            LeadLagDirection::TargetLeads => summary.target_lead_count += 1,
            LeadLagDirection::TargetLags => summary.target_lag_count += 1,
            LeadLagDirection::Equal => summary.equal_count += 1,
            LeadLagDirection::MissingComparison => summary.missing_comparison_count += 1,
        }
        if let Some(delta) = row.delta_magnitude {
            summary.minimum_delta_magnitude = Some(
                summary
                    .minimum_delta_magnitude
                    .map_or(delta, |current| current.min(delta)),
            );
            summary.maximum_delta_magnitude = Some(
                summary
                    .maximum_delta_magnitude
                    .map_or(delta, |current| current.max(delta)),
            );
        }
    }

    (rows, summary)
}

fn find_nearest_transition<'a>(
    candidates: &'a [TransitionPoint<'a>],
    target_point: i64,
) -> TransitionPoint<'a> {
    let mut best = candidates[0];
    let mut best_distance = (target_point - best.point).abs();
    for &candidate in &candidates[1..] {
        let distance = (target_point - candidate.point).abs();
        if distance < best_distance {
            best = candidate;
            best_distance = distance;
        }
    }
    best
}

fn direction_for_delta(delta: i64) -> LeadLagDirection {
    if delta < 0 {
        LeadLagDirection::TargetLeads
    } else if delta > 0 {
        LeadLagDirection::TargetLags
    } else {
        LeadLagDirection::Equal
    }
}

fn build_as_of_rows(
    groups: &BTreeMap<GroupKey, GroupWindows<'_>>,
    direction: AsOfDirection,
    axis: TemporalAxis,
    tolerance_magnitude: i64,
) -> (Vec<AsOfRow>, Vec<ComparisonDiagnostic>) {
    if axis != TemporalAxis::ProcessingPosition {
        return (Vec::new(), Vec::new());
    }

    let mut rows = Vec::new();
    let mut diagnostics = Vec::new();
    for ((window_name, key, partition), (targets, againsts)) in groups {
        let mut candidates: Vec<TransitionPoint<'_>> = againsts
            .iter()
            .map(|against| TransitionPoint {
                record_id: against.record_id,
                point: against.start,
            })
            .collect();
        candidates.sort_by_key(|item| (item.point, item.record_id));

        for target in targets {
            let target_point = target.start;
            let target_point_row = RowPoint {
                axis,
                magnitude: target_point,
            };

            if candidates.is_empty() {
                rows.push(AsOfRow {
                    window_name: window_name.clone(),
                    key: key.clone(),
                    partition: partition.clone(),
                    axis,
                    direction: direction.clone(),
                    target_point: target_point_row,
                    matched_point: None,
                    distance_magnitude: None,
                    tolerance_magnitude,
                    status: AsOfMatchStatus::NoMatch,
                    target_record_id: target.record_id.to_owned(),
                    matched_record_id: None,
                });
                continue;
            }

            let (best, ambiguous, future_rejected) =
                find_as_of_candidate(&candidates, target_point, &direction);
            let Some(best) = best else {
                rows.push(AsOfRow {
                    window_name: window_name.clone(),
                    key: key.clone(),
                    partition: partition.clone(),
                    axis,
                    direction: direction.clone(),
                    target_point: target_point_row,
                    matched_point: None,
                    distance_magnitude: future_rejected
                        .map(|item| (target_point - item.point).abs()),
                    tolerance_magnitude,
                    status: if future_rejected.is_some() {
                        AsOfMatchStatus::FutureRejected
                    } else {
                        AsOfMatchStatus::NoMatch
                    },
                    target_record_id: target.record_id.to_owned(),
                    matched_record_id: None,
                });
                continue;
            };

            let distance = (target_point - best.point).abs();
            if distance > tolerance_magnitude {
                rows.push(AsOfRow {
                    window_name: window_name.clone(),
                    key: key.clone(),
                    partition: partition.clone(),
                    axis,
                    direction: direction.clone(),
                    target_point: target_point_row,
                    matched_point: None,
                    distance_magnitude: Some(distance),
                    tolerance_magnitude,
                    status: AsOfMatchStatus::NoMatch,
                    target_record_id: target.record_id.to_owned(),
                    matched_record_id: None,
                });
                continue;
            }

            if ambiguous {
                diagnostics.push(ComparisonDiagnostic {
                    code: "AmbiguousAsOfMatch".to_owned(),
                    severity: DiagnosticSeverity::Warning,
                });
            }

            rows.push(AsOfRow {
                window_name: window_name.clone(),
                key: key.clone(),
                partition: partition.clone(),
                axis,
                direction: direction.clone(),
                target_point: target_point_row,
                matched_point: Some(RowPoint {
                    axis,
                    magnitude: best.point,
                }),
                distance_magnitude: Some(distance),
                tolerance_magnitude,
                status: if ambiguous {
                    AsOfMatchStatus::Ambiguous
                } else if distance == 0 {
                    AsOfMatchStatus::Exact
                } else {
                    AsOfMatchStatus::Matched
                },
                target_record_id: target.record_id.to_owned(),
                matched_record_id: Some(best.record_id.to_owned()),
            });
        }
    }

    (rows, diagnostics)
}

fn find_as_of_candidate<'a>(
    candidates: &'a [TransitionPoint<'a>],
    target_point: i64,
    direction: &AsOfDirection,
) -> (
    Option<TransitionPoint<'a>>,
    bool,
    Option<TransitionPoint<'a>>,
) {
    let mut ambiguous = false;
    let mut future_rejected = None;
    let mut best = None;
    let mut best_distance = None;

    for &candidate in candidates {
        let comparison = candidate.point.cmp(&target_point);
        if *direction == AsOfDirection::Previous && comparison.is_gt() {
            future_rejected.get_or_insert(candidate);
            continue;
        }
        if *direction == AsOfDirection::Next && comparison.is_lt() {
            continue;
        }

        let distance = (target_point - candidate.point).abs();
        if best_distance.is_none_or(|current| distance < current) {
            best = Some(candidate);
            best_distance = Some(distance);
            ambiguous = false;
            continue;
        }

        if Some(distance) == best_distance {
            ambiguous = true;
            if best.is_some_and(|current| candidate.record_id < current.record_id) {
                best = Some(candidate);
            }
        }
    }

    (best, ambiguous, future_rejected)
}

#[cfg(test)]
mod tests {
    use crate::{WindowHistoryFixture, fixture::ContractFixture};

    use super::*;

    #[test]
    fn basic_overlap_fixture_matches_expected_counts() {
        let fixture = ContractFixture::parse_json(include_str!(
            "../../../../dotnet/tests/Spanfold.Tests/Comparison/Fixtures/basic-overlap.json"
        ))
        .expect("fixture should parse");
        let result = compare(fixture.history(), fixture.plan());

        assert!(result.is_valid);
        assert_eq!(result.comparator_summaries[0].row_count, 1);
        assert_eq!(result.comparator_summaries[1].row_count, 1);
        assert_eq!(result.comparator_summaries[2].row_count, 2);
        assert_eq!(result.overlap_rows[0].range, RowRange { start: 3, end: 5 });
        assert_eq!(result.residual_rows[0].range, RowRange { start: 1, end: 3 });
    }

    #[test]
    fn gap_and_symmetric_difference_match_expected_rows() {
        let history = WindowHistoryFixture::new()
            .closed_window("DeviceOffline", "device-1", 1, 3, |w| {
                w.source("provider-a")
            })
            .expect("target")
            .closed_window("DeviceOffline", "device-1", 5, 7, |w| {
                w.source("provider-b")
            })
            .expect("against")
            .build();
        let plan = ComparisonPlan {
            name: "Provider QA".to_owned(),
            target_source: "provider-a".to_owned(),
            against: AgainstSelection::Sources(vec!["provider-b".to_owned()]),
            scope_window: Some("DeviceOffline".to_owned()),
            scope_segments: Vec::new(),
            scope_tags: Vec::new(),
            comparators: vec![Comparator::Gap, Comparator::SymmetricDifference],
            known_at: None,
            open_window_policy: OpenWindowPolicy::RequireClosed,
            open_window_horizon: None,
            strict: false,
        };

        let result = compare(&history, &plan);

        assert_eq!(result.gap_rows.len(), 1);
        assert_eq!(result.gap_rows[0].range, RowRange { start: 3, end: 5 });
        assert_eq!(result.symmetric_difference_rows.len(), 2);
        assert_eq!(
            result.symmetric_difference_rows[0].side,
            ComparisonSide::Target
        );
        assert_eq!(
            result.symmetric_difference_rows[1].side,
            ComparisonSide::Against
        );
    }

    #[test]
    fn containment_emits_left_contained_and_right_rows() {
        let history = WindowHistoryFixture::new()
            .closed_window("DeviceOffline", "device-1", 1, 7, |w| w.source("target"))
            .expect("target")
            .closed_window("DeviceOffline", "device-1", 3, 5, |w| w.source("container"))
            .expect("container")
            .build();
        let plan = ComparisonPlan {
            name: "Containment".to_owned(),
            target_source: "target".to_owned(),
            against: AgainstSelection::Sources(vec!["container".to_owned()]),
            scope_window: Some("DeviceOffline".to_owned()),
            scope_segments: Vec::new(),
            scope_tags: Vec::new(),
            comparators: vec![Comparator::Containment],
            known_at: None,
            open_window_policy: OpenWindowPolicy::RequireClosed,
            open_window_horizon: None,
            strict: false,
        };

        let result = compare(&history, &plan);
        assert_eq!(result.containment_rows.len(), 3);
        assert_eq!(
            result.containment_rows[0].status,
            ContainmentStatus::LeftOverhang
        );
        assert_eq!(
            result.containment_rows[1].status,
            ContainmentStatus::Contained
        );
        assert_eq!(
            result.containment_rows[2].status,
            ContainmentStatus::RightOverhang
        );
    }

    #[test]
    fn lead_lag_and_as_of_emit_expected_rows() {
        let history = WindowHistoryFixture::new()
            .closed_window("DeviceOffline", "device-1", 1, 4, |w| w.source("target"))
            .expect("target")
            .closed_window("DeviceOffline", "device-1", 3, 6, |w| {
                w.source("comparison")
            })
            .expect("comparison")
            .closed_window("Quote", "selection-1", 10, 11, |w| w.source("trade"))
            .expect("trade")
            .closed_window("Quote", "selection-1", 7, 20, |w| w.source("quote"))
            .expect("quote")
            .build();

        let lead_lag = compare(
            &history,
            &ComparisonPlan {
                name: "Latency QA".to_owned(),
                target_source: "target".to_owned(),
                against: AgainstSelection::Sources(vec!["comparison".to_owned()]),
                scope_window: Some("DeviceOffline".to_owned()),
                scope_segments: Vec::new(),
                scope_tags: Vec::new(),
                comparators: vec![Comparator::LeadLag {
                    transition: LeadLagTransition::Start,
                    axis: TemporalAxis::ProcessingPosition,
                    tolerance_magnitude: 5,
                }],
                known_at: None,
                open_window_policy: OpenWindowPolicy::RequireClosed,
                open_window_horizon: None,
                strict: false,
            },
        );
        assert_eq!(lead_lag.lead_lag_rows.len(), 1);
        assert_eq!(
            lead_lag.lead_lag_rows[0].direction,
            LeadLagDirection::TargetLeads
        );
        assert_eq!(lead_lag.lead_lag_rows[0].delta_magnitude, Some(-2));
        assert_eq!(lead_lag.lead_lag_summaries[0].target_lead_count, 1);

        let as_of = compare(
            &history,
            &ComparisonPlan {
                name: "Quote at trade".to_owned(),
                target_source: "trade".to_owned(),
                against: AgainstSelection::Sources(vec!["quote".to_owned()]),
                scope_window: Some("Quote".to_owned()),
                scope_segments: Vec::new(),
                scope_tags: Vec::new(),
                comparators: vec![Comparator::AsOf {
                    direction: AsOfDirection::Previous,
                    axis: TemporalAxis::ProcessingPosition,
                    tolerance_magnitude: 5,
                }],
                known_at: None,
                open_window_policy: OpenWindowPolicy::RequireClosed,
                open_window_horizon: None,
                strict: false,
            },
        );
        assert_eq!(as_of.as_of_rows.len(), 1);
        assert_eq!(as_of.as_of_rows[0].status, AsOfMatchStatus::Matched);
        assert_eq!(as_of.as_of_rows[0].distance_magnitude, Some(3));
    }

    #[test]
    fn residual_against_all_cohort_requires_every_member_active() {
        let history = WindowHistoryFixture::new()
            .closed_window("SelectionPriced", "selection-1", 1, 11, |w| {
                w.source("source-a")
            })
            .expect("target")
            .closed_window("SelectionPriced", "selection-1", 1, 6, |w| {
                w.source("source-b")
            })
            .expect("b")
            .closed_window("SelectionPriced", "selection-1", 6, 11, |w| {
                w.source("source-c")
            })
            .expect("c")
            .build();

        let result = compare(
            &history,
            &ComparisonPlan {
                name: "cohort all".to_owned(),
                target_source: "source-a".to_owned(),
                against: AgainstSelection::Cohort {
                    name: "cohort".to_owned(),
                    sources: vec!["source-b".to_owned(), "source-c".to_owned()],
                    activity: CohortActivity::All,
                },
                scope_window: Some("SelectionPriced".to_owned()),
                scope_segments: Vec::new(),
                scope_tags: Vec::new(),
                comparators: vec![Comparator::Residual],
                known_at: None,
                open_window_policy: OpenWindowPolicy::RequireClosed,
                open_window_horizon: None,
                strict: false,
            },
        );

        let total: i64 = result
            .residual_rows
            .iter()
            .map(|row| row.range.end - row.range.start)
            .sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn residual_against_threshold_and_none_cohorts_use_activity_rules() {
        let threshold_history = WindowHistoryFixture::new()
            .closed_window("SelectionPriced", "selection-1", 1, 11, |w| {
                w.source("source-a")
            })
            .expect("target")
            .closed_window("SelectionPriced", "selection-1", 1, 11, |w| {
                w.source("source-b")
            })
            .expect("b")
            .closed_window("SelectionPriced", "selection-1", 1, 6, |w| {
                w.source("source-c")
            })
            .expect("c")
            .closed_window("SelectionPriced", "selection-1", 6, 11, |w| {
                w.source("source-d")
            })
            .expect("d")
            .build();

        let threshold = compare(
            &threshold_history,
            &ComparisonPlan {
                name: "cohort at least".to_owned(),
                target_source: "source-a".to_owned(),
                against: AgainstSelection::Cohort {
                    name: "cohort".to_owned(),
                    sources: vec![
                        "source-b".to_owned(),
                        "source-c".to_owned(),
                        "source-d".to_owned(),
                    ],
                    activity: CohortActivity::AtLeast { count: 2 },
                },
                scope_window: Some("SelectionPriced".to_owned()),
                scope_segments: Vec::new(),
                scope_tags: Vec::new(),
                comparators: vec![Comparator::Residual],
                known_at: None,
                open_window_policy: OpenWindowPolicy::RequireClosed,
                open_window_horizon: None,
                strict: false,
            },
        );
        assert!(threshold.residual_rows.is_empty());

        let none_history = WindowHistoryFixture::new()
            .closed_window("SelectionPriced", "selection-1", 1, 11, |w| {
                w.source("source-a")
            })
            .expect("target")
            .closed_window("SelectionPriced", "selection-1", 1, 6, |w| {
                w.source("source-b")
            })
            .expect("b")
            .build();

        let none = compare(
            &none_history,
            &ComparisonPlan {
                name: "cohort none".to_owned(),
                target_source: "source-a".to_owned(),
                against: AgainstSelection::Cohort {
                    name: "cohort".to_owned(),
                    sources: vec!["source-b".to_owned(), "source-c".to_owned()],
                    activity: CohortActivity::None,
                },
                scope_window: Some("SelectionPriced".to_owned()),
                scope_segments: Vec::new(),
                scope_tags: Vec::new(),
                comparators: vec![Comparator::Residual],
                known_at: None,
                open_window_policy: OpenWindowPolicy::RequireClosed,
                open_window_horizon: None,
                strict: false,
            },
        );
        let total: i64 = none
            .residual_rows
            .iter()
            .map(|row| row.range.end - row.range.start)
            .sum();
        assert_eq!(total, 5);
    }

    #[test]
    fn live_open_windows_emit_provisional_row_finality() {
        let history = WindowHistoryFixture::new()
            .open_window("DeviceOffline", "device-1", 1, |w| w.source("provider-a"))
            .closed_window("DeviceOffline", "device-1", 3, 5, |w| {
                w.source("provider-b")
            })
            .expect("provider-b")
            .build();
        let plan = ComparisonPlan {
            name: "Live QA".to_owned(),
            target_source: "provider-a".to_owned(),
            against: AgainstSelection::Sources(vec!["provider-b".to_owned()]),
            scope_window: Some("DeviceOffline".to_owned()),
            scope_segments: Vec::new(),
            scope_tags: Vec::new(),
            comparators: vec![Comparator::Residual],
            known_at: None,
            open_window_policy: OpenWindowPolicy::ClipToHorizon,
            open_window_horizon: Some(crate::TemporalPoint::position(10)),
            strict: false,
        };

        let result = compare_live(&history, &plan, crate::TemporalPoint::position(10));

        assert_eq!(result.residual_rows.len(), 2);
        assert!(result.has_provisional_rows());
        assert_eq!(result.provisional_row_finalities().len(), 2);
        assert_eq!(
            result.provisional_row_finalities()[0].finality,
            ComparisonFinality::Provisional
        );
    }

    #[test]
    fn prepare_excludes_future_windows_at_known_at_position() {
        let history = WindowHistoryFixture::new()
            .closed_window("DeviceOffline", "device-1", 1, 5, |w| {
                w.source("provider-a").known_at_position(10)
            })
            .expect("window")
            .build();
        let plan = ComparisonPlan {
            name: "Decision audit".to_owned(),
            target_source: "provider-a".to_owned(),
            against: AgainstSelection::Sources(vec!["provider-b".to_owned()]),
            scope_window: Some("DeviceOffline".to_owned()),
            scope_segments: Vec::new(),
            scope_tags: Vec::new(),
            comparators: vec![Comparator::Overlap],
            known_at: Some(crate::TemporalPoint::position(5)),
            open_window_policy: OpenWindowPolicy::RequireClosed,
            open_window_horizon: None,
            strict: false,
        };

        let prepared = prepare(&history, &plan);

        assert!(prepared.normalized_windows.is_empty());
        assert_eq!(prepared.excluded_windows.len(), 1);
        assert_eq!(
            prepared.excluded_windows[0].diagnostic_code.as_deref(),
            Some("FutureWindowExcluded")
        );
    }

    #[test]
    fn cohort_comparison_emits_extension_metadata_and_parsed_evidence() {
        let history = WindowHistoryFixture::new()
            .closed_window("SelectionPriced", "selection-1", 1, 11, |w| {
                w.source("source-a")
            })
            .expect("target")
            .closed_window("SelectionPriced", "selection-1", 1, 6, |w| {
                w.source("source-b")
            })
            .expect("b")
            .closed_window("SelectionPriced", "selection-1", 6, 11, |w| {
                w.source("source-c")
            })
            .expect("c")
            .build();

        let result = compare(
            &history,
            &ComparisonPlan {
                name: "cohort evidence".to_owned(),
                target_source: "source-a".to_owned(),
                against: AgainstSelection::Cohort {
                    name: "cohort".to_owned(),
                    sources: vec!["source-b".to_owned(), "source-c".to_owned()],
                    activity: CohortActivity::All,
                },
                scope_window: Some("SelectionPriced".to_owned()),
                scope_segments: Vec::new(),
                scope_tags: Vec::new(),
                comparators: vec![Comparator::Residual],
                known_at: None,
                open_window_policy: OpenWindowPolicy::RequireClosed,
                open_window_horizon: None,
                strict: false,
            },
        );

        assert!(!result.extension_metadata.is_empty());
        let evidence = result.cohort_evidence();
        assert_eq!(evidence.len(), 2);
        assert!(!evidence[0].is_active);
        assert!(!evidence[1].is_active);
    }
}
