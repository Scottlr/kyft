use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::{PrimitiveValue, WindowHistory};

/// Comparator family supported by the current Rust implementation.
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
}

impl Comparator {
    /// Parses a comparator declaration.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "overlap" => Some(Self::Overlap),
            "residual" => Some(Self::Residual),
            "missing" => Some(Self::Missing),
            "coverage" => Some(Self::Coverage),
            _ => None,
        }
    }

    /// Returns the export name.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Overlap => "overlap",
            Self::Residual => "residual",
            Self::Missing => "missing",
            Self::Coverage => "coverage",
        }
    }
}

/// Comparison-side selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AgainstSelection {
    /// One or more source lanes.
    Sources(Vec<String>),
    /// Cohort activity across sources.
    CohortAny {
        /// Exported cohort name.
        name: String,
        /// Participating sources.
        sources: Vec<String>,
    },
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
    /// Whether strict validation is enabled.
    pub strict: bool,
}

/// Structured comparison diagnostic.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ComparisonDiagnostic {
    /// Diagnostic code.
    pub code: String,
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
}

/// Structured comparison result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ComparisonResult {
    /// Result schema.
    pub schema: String,
    /// Schema version.
    #[serde(rename = "schemaVersion")]
    pub schema_version: u32,
    /// Artifact kind.
    pub artifact: String,
    /// Whether the result is valid.
    #[serde(rename = "isValid")]
    pub is_valid: bool,
    /// Diagnostics.
    pub diagnostics: Vec<ComparisonDiagnostic>,
    /// Comparator summaries.
    #[serde(rename = "comparatorSummaries")]
    pub comparator_summaries: Vec<ComparatorSummary>,
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
}

#[derive(Clone, Debug)]
struct SegmentRef<'a> {
    start: i64,
    end: i64,
    record_id: &'a str,
}

type GroupKey = (String, String, Option<String>);
type GroupWindows<'a> = (Vec<SegmentRef<'a>>, Vec<SegmentRef<'a>>);

/// Executes a comparison over closed windows.
#[must_use]
pub fn compare(history: &WindowHistory, plan: &ComparisonPlan) -> ComparisonResult {
    let mut diagnostics = Vec::new();
    if plan.strict && plan.scope_window.is_none() {
        diagnostics.push(ComparisonDiagnostic {
            code: "BroadSelector".to_owned(),
        });
    }

    if !diagnostics.is_empty() {
        return ComparisonResult {
            schema: "spanfold.comparison.result".to_owned(),
            schema_version: 0,
            artifact: "result".to_owned(),
            is_valid: false,
            diagnostics,
            comparator_summaries: Vec::new(),
            overlap_rows: Vec::new(),
            residual_rows: Vec::new(),
            missing_rows: Vec::new(),
            coverage_rows: Vec::new(),
        };
    }

    let rows = compare_rows(history, plan);
    let mut summaries = Vec::new();
    for comparator in &plan.comparators {
        let row_count = match comparator {
            Comparator::Overlap => rows.overlap.len(),
            Comparator::Residual => rows.residual.len(),
            Comparator::Missing => rows.missing.len(),
            Comparator::Coverage => rows.coverage.len(),
        };
        summaries.push(ComparatorSummary {
            comparator_name: comparator.as_str().to_owned(),
            row_count,
        });
    }

    ComparisonResult {
        schema: "spanfold.comparison.result".to_owned(),
        schema_version: 0,
        artifact: "result".to_owned(),
        is_valid: true,
        diagnostics,
        comparator_summaries: summaries,
        overlap_rows: rows.overlap,
        residual_rows: rows.residual,
        missing_rows: rows.missing,
        coverage_rows: rows.coverage,
    }
}

fn compare_rows(history: &WindowHistory, plan: &ComparisonPlan) -> ComparisonRows {
    let mut groups: BTreeMap<GroupKey, GroupWindows<'_>> = BTreeMap::new();

    for window in history.closed_windows() {
        if let Some(scope_window) = &plan.scope_window
            && window.window_name != *scope_window
        {
            continue;
        }

        if !matches_filters(window, &plan.scope_segments, &plan.scope_tags) {
            continue;
        }

        let is_target = window.source.as_deref() == Some(plan.target_source.as_str());
        let is_against = match &plan.against {
            AgainstSelection::Sources(sources) => window
                .source
                .as_ref()
                .is_some_and(|source| sources.iter().any(|item| item == source)),
            AgainstSelection::CohortAny { sources, .. } => window
                .source
                .as_ref()
                .is_some_and(|source| sources.iter().any(|item| item == source)),
        };

        if !is_target && !is_against {
            continue;
        }

        let group = groups
            .entry((
                window.window_name.clone(),
                window.key.clone(),
                window.partition.clone(),
            ))
            .or_default();
        let segment = SegmentRef {
            start: window.range.start().magnitude(),
            end: window.range.end().magnitude(),
            record_id: window.id.as_str(),
        };

        if is_target {
            group.0.push(segment.clone());
        }
        if is_against {
            group.1.push(segment);
        }
    }

    let mut rows = ComparisonRows::default();
    for ((window_name, key, partition), (targets, againsts)) in groups {
        if plan.comparators.contains(&Comparator::Overlap) {
            rows.overlap.extend(overlap_rows(
                &window_name,
                &key,
                partition.clone(),
                &targets,
                &againsts,
            ));
        }
        if plan.comparators.contains(&Comparator::Residual) {
            rows.residual.extend(residual_rows(
                &window_name,
                &key,
                partition.clone(),
                &targets,
                &againsts,
            ));
        }
        if plan.comparators.contains(&Comparator::Missing) {
            rows.missing.extend(missing_rows(
                &window_name,
                &key,
                partition.clone(),
                &targets,
                &againsts,
            ));
        }
        if plan.comparators.contains(&Comparator::Coverage) {
            rows.coverage.extend(coverage_rows(
                &window_name,
                &key,
                partition,
                &targets,
                &againsts,
            ));
        }
    }

    rows
}

fn matches_filters(
    window: &crate::ClosedWindow,
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

fn overlap_rows(
    window_name: &str,
    key: &str,
    partition: Option<String>,
    targets: &[SegmentRef<'_>],
    againsts: &[SegmentRef<'_>],
) -> Vec<OverlapRow> {
    let mut rows = Vec::new();
    for target in targets {
        for against in againsts {
            let start = target.start.max(against.start);
            let end = target.end.min(against.end);
            if start < end {
                rows.push(OverlapRow {
                    window_name: window_name.to_owned(),
                    key: key.to_owned(),
                    partition: partition.clone(),
                    range: RowRange { start, end },
                    target_record_ids: vec![target.record_id.to_owned()],
                    against_record_ids: vec![against.record_id.to_owned()],
                });
            }
        }
    }
    rows.sort_by_key(|row| (row.range.start, row.range.end));
    rows
}

fn residual_rows(
    window_name: &str,
    key: &str,
    partition: Option<String>,
    targets: &[SegmentRef<'_>],
    againsts: &[SegmentRef<'_>],
) -> Vec<ResidualRow> {
    let against_intervals: Vec<(i64, i64)> =
        againsts.iter().map(|item| (item.start, item.end)).collect();
    let mut rows = Vec::new();
    for target in targets {
        for (start, end) in subtract_intervals((target.start, target.end), &against_intervals) {
            rows.push(ResidualRow {
                window_name: window_name.to_owned(),
                key: key.to_owned(),
                partition: partition.clone(),
                range: RowRange { start, end },
                target_record_ids: vec![target.record_id.to_owned()],
            });
        }
    }
    rows.sort_by_key(|row| (row.range.start, row.range.end));
    rows
}

fn missing_rows(
    window_name: &str,
    key: &str,
    partition: Option<String>,
    targets: &[SegmentRef<'_>],
    againsts: &[SegmentRef<'_>],
) -> Vec<MissingRow> {
    let target_intervals: Vec<(i64, i64)> =
        targets.iter().map(|item| (item.start, item.end)).collect();
    let mut rows = Vec::new();
    for against in againsts {
        for (start, end) in subtract_intervals((against.start, against.end), &target_intervals) {
            rows.push(MissingRow {
                window_name: window_name.to_owned(),
                key: key.to_owned(),
                partition: partition.clone(),
                range: RowRange { start, end },
                against_record_ids: vec![against.record_id.to_owned()],
            });
        }
    }
    rows.sort_by_key(|row| (row.range.start, row.range.end));
    rows
}

fn coverage_rows(
    window_name: &str,
    key: &str,
    partition: Option<String>,
    targets: &[SegmentRef<'_>],
    againsts: &[SegmentRef<'_>],
) -> Vec<CoverageRow> {
    let mut rows = Vec::new();
    for target in targets {
        let mut points = BTreeSet::from([target.start, target.end]);
        for against in againsts {
            let start = target.start.max(against.start);
            let end = target.end.min(against.end);
            if start < end {
                points.insert(start);
                points.insert(end);
            }
        }

        let points: Vec<i64> = points.into_iter().collect();
        for pair in points.windows(2) {
            let start = pair[0];
            let end = pair[1];
            if start >= end {
                continue;
            }
            let covered_ids: Vec<String> = againsts
                .iter()
                .filter(|against| against.start < end && against.end > start)
                .map(|against| against.record_id.to_owned())
                .collect();
            rows.push(CoverageRow {
                window_name: window_name.to_owned(),
                key: key.to_owned(),
                partition: partition.clone(),
                range: RowRange { start, end },
                target_magnitude: end - start,
                covered_magnitude: if covered_ids.is_empty() {
                    0
                } else {
                    end - start
                },
                target_record_ids: vec![target.record_id.to_owned()],
                against_record_ids: covered_ids,
            });
        }
    }
    rows.sort_by_key(|row| (row.range.start, row.range.end));
    rows
}

fn subtract_intervals(base: (i64, i64), subtract: &[(i64, i64)]) -> Vec<(i64, i64)> {
    let mut remaining = vec![base];
    for &(sub_start, sub_end) in subtract {
        let mut next = Vec::new();
        for (start, end) in remaining {
            if sub_end <= start || sub_start >= end {
                next.push((start, end));
                continue;
            }
            if sub_start > start {
                next.push((start, sub_start));
            }
            if sub_end < end {
                next.push((sub_end, end));
            }
        }
        remaining = next;
    }
    remaining
        .into_iter()
        .filter(|(start, end)| start < end)
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::fixture::ContractFixture;

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
}
