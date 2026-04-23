use std::str::FromStr;

use serde::Deserialize;
use thiserror::Error;

use crate::{
    AgainstSelection, Comparator, ComparisonPlan, PrimitiveValue, WindowFilter, WindowHistory,
    WindowHistoryFixture, compare,
};

/// Fixture loading and validation error.
#[derive(Debug, Error)]
pub enum FixtureError {
    /// JSON parse failure.
    #[error("failed to parse fixture JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// Validation failure.
    #[error("{0}")]
    Validation(String),
    /// Temporal range construction failure.
    #[error("{0}")]
    Temporal(#[from] crate::TemporalRangeError),
}

/// Parsed contract fixture.
#[derive(Clone, Debug)]
pub struct ContractFixture {
    history: WindowHistory,
    plan: ComparisonPlan,
}

impl ContractFixture {
    /// Parses a fixture JSON document.
    pub fn parse_json(json: &str) -> Result<Self, FixtureError> {
        let parsed: RawFixture = serde_json::from_str(json)?;
        parsed.validate()?;
        let mut builder = WindowHistoryFixture::new();
        for window in parsed.windows {
            let metadata = |w| apply_window_metadata(w, &window);
            if let Some(end) = window.end_position {
                builder = builder.closed_window(
                    window.window_name.clone(),
                    window.key.clone(),
                    window.start_position,
                    end,
                    metadata,
                )?;
            } else {
                builder = builder.open_window(
                    window.window_name.clone(),
                    window.key.clone(),
                    window.start_position,
                    metadata,
                );
            }
        }

        Ok(Self {
            history: builder.build(),
            plan: parsed.plan.try_into()?,
        })
    }

    /// Returns the fixture history.
    #[must_use]
    pub fn history(&self) -> &WindowHistory {
        &self.history
    }

    /// Returns the fixture plan.
    #[must_use]
    pub fn plan(&self) -> &ComparisonPlan {
        &self.plan
    }

    /// Executes the fixture.
    #[must_use]
    pub fn execute(&self) -> crate::ComparisonResult {
        compare(&self.history, &self.plan)
    }
}

impl FromStr for ContractFixture {
    type Err = FixtureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_json(s)
    }
}

#[derive(Clone, Debug, Deserialize)]
struct RawFixture {
    schema: String,
    #[serde(rename = "schemaVersion")]
    schema_version: u32,
    windows: Vec<RawWindow>,
    plan: RawPlan,
}

impl RawFixture {
    fn validate(&self) -> Result<(), FixtureError> {
        if self.schema != "spanfold.contract-fixture" {
            return Err(FixtureError::Validation(
                "$.schema must be spanfold.contract-fixture.".to_owned(),
            ));
        }
        if self.schema_version != 1 {
            return Err(FixtureError::Validation(
                "$.schemaVersion must be 1.".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
struct RawWindow {
    #[serde(rename = "windowName")]
    window_name: String,
    key: String,
    source: Option<String>,
    partition: Option<String>,
    #[serde(rename = "startPosition")]
    start_position: i64,
    #[serde(rename = "endPosition")]
    end_position: Option<i64>,
    #[serde(default)]
    segments: Vec<RawNamedValue>,
    #[serde(default)]
    tags: Vec<RawNamedValue>,
}

#[derive(Clone, Debug, Deserialize)]
struct RawNamedValue {
    name: String,
    value: PrimitiveValue,
    #[serde(rename = "parentName")]
    parent_name: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct RawPlan {
    name: String,
    #[serde(rename = "targetSource")]
    target_source: String,
    #[serde(rename = "againstSources")]
    against_sources: Option<Vec<String>>,
    #[serde(rename = "againstCohort")]
    against_cohort: Option<RawAgainstCohort>,
    #[serde(rename = "scopeWindow")]
    scope_window: Option<String>,
    #[serde(rename = "scopeSegments", default)]
    scope_segments: Vec<RawNamedValue>,
    #[serde(rename = "scopeTags", default)]
    scope_tags: Vec<RawNamedValue>,
    comparators: Vec<String>,
    strict: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct RawAgainstCohort {
    name: String,
    sources: Vec<String>,
    activity: String,
}

impl TryFrom<RawPlan> for ComparisonPlan {
    type Error = FixtureError;

    fn try_from(value: RawPlan) -> Result<Self, Self::Error> {
        let against = if let Some(cohort) = value.against_cohort {
            if cohort.activity != "any" {
                return Err(FixtureError::Validation(
                    "only againstCohort activity=any is implemented in Rust so far.".to_owned(),
                ));
            }
            AgainstSelection::CohortAny {
                name: cohort.name,
                sources: cohort.sources,
            }
        } else if let Some(sources) = value.against_sources {
            AgainstSelection::Sources(sources)
        } else {
            return Err(FixtureError::Validation(
                "$.plan must contain againstSources or againstCohort.".to_owned(),
            ));
        };

        let mut comparators = Vec::new();
        for comparator in value.comparators {
            let parsed = Comparator::parse(&comparator).ok_or_else(|| {
                FixtureError::Validation(format!("unsupported comparator: {comparator}"))
            })?;
            comparators.push(parsed);
        }

        Ok(ComparisonPlan {
            name: value.name,
            target_source: value.target_source,
            against,
            scope_window: value.scope_window,
            scope_segments: into_filters(value.scope_segments),
            scope_tags: into_filters(value.scope_tags),
            comparators,
            strict: value.strict,
        })
    }
}

fn into_filters(values: Vec<RawNamedValue>) -> Vec<WindowFilter> {
    values
        .into_iter()
        .map(|item| WindowFilter {
            name: item.name,
            value: item.value,
        })
        .collect()
}

fn apply_window_metadata(
    mut builder: crate::WindowHistoryFixtureWindow,
    window: &RawWindow,
) -> crate::WindowHistoryFixtureWindow {
    if let Some(source) = &window.source {
        builder = builder.source(source.clone());
    }
    if let Some(partition) = &window.partition {
        builder = builder.partition(partition.clone());
    }
    for segment in &window.segments {
        builder = if let Some(parent) = &segment.parent_name {
            builder.child_segment(segment.name.clone(), segment.value.clone(), parent.clone())
        } else {
            builder.segment(segment.name.clone(), segment.value.clone())
        };
    }
    for tag in &window.tags {
        builder = builder.tag(tag.name.clone(), tag.value.clone());
    }
    builder
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segmented_fixture_filters_against_windows_out_of_scope() {
        let fixture = ContractFixture::parse_json(include_str!(
            "../../../../dotnet/tests/Spanfold.Tests/Comparison/Fixtures/segmented-residual.json"
        ))
        .expect("fixture should parse");
        let result = fixture.execute();

        assert!(result.is_valid);
        assert_eq!(result.residual_rows.len(), 1);
        assert_eq!(result.residual_rows[0].range.start, 1);
        assert_eq!(result.residual_rows[0].range.end, 5);
    }

    #[test]
    fn strict_broad_scope_fixture_emits_broad_selector_diagnostic() {
        let fixture = ContractFixture::parse_json(include_str!(
            "../../../../dotnet/tests/Spanfold.Tests/Comparison/Fixtures/strict-broad-scope.json"
        ))
        .expect("fixture should parse");
        let result = fixture.execute();

        assert!(!result.is_valid);
        assert_eq!(result.diagnostics[0].code, "BroadSelector");
    }

    #[test]
    fn cohort_any_fixture_parses_and_executes() {
        let fixture = ContractFixture::parse_json(include_str!(
            "../../../../dotnet/tests/Spanfold.Tests/Comparison/Fixtures/cohort-any-residual.json"
        ))
        .expect("fixture should parse");
        let result = fixture.execute();

        assert!(result.is_valid);
        assert!(result.residual_rows.is_empty());
    }
}
