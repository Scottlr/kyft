#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! Core Rust primitives for Spanfold.
//!
//! This crate is the start of Spanfold's Rust 1.95.0 / Rust 2024
//! implementation. It intentionally begins with strongly typed data structures
//! and builders that future comparison, export, and CLI work can use without a
//! mechanical translation from the .NET or Python codebases.

mod analytics;
mod builders;
mod changelog;
mod comparison;
mod explain;
mod export;
mod extensions;
mod fixture;
mod pipeline;
mod primitive;
mod records;
mod temporal;

pub use analytics::{
    HierarchyComparisonResult, HierarchyComparisonRow, HierarchyComparisonRowKind,
    SourceMatrixCell, SourceMatrixResult, compare_hierarchy, compare_sources,
};
pub use builders::WindowComparisonBuilder;
pub use changelog::{ComparisonChangelogEntry, create_changelog, replay_changelog};
pub use comparison::{
    AgainstSelection, AlignedComparison, AlignedSegmentArtifact, AsOfDirection, AsOfMatchStatus,
    AsOfRow, CohortActivity, Comparator, ComparatorSummary, ComparisonDiagnostic,
    ComparisonFinality, ComparisonPlan, ComparisonResult, ComparisonRowFinality, ComparisonRows,
    ComparisonSide, ContainmentRow, ContainmentStatus, CoverageRow, CoverageSummary,
    DiagnosticSeverity, ExcludedWindowRecord, GapRow, LeadLagDirection, LeadLagRow, LeadLagSummary,
    LeadLagTransition, MissingRow, NormalizedWindowRecord, OpenWindowPolicy, OverlapRow,
    PreparedComparison, ResidualRow, RowPoint, RowRange, SymmetricDifferenceRow, WindowArtifact,
    WindowFilter, align, compare, compare_live, prepare, prepare_live,
};
pub use explain::ComparisonExplanationFormat;
pub use export::{
    export_plan_json, export_result_debug_html, export_result_json, export_result_json_lines,
    export_result_llm_context, export_result_markdown,
};
pub use extensions::{
    CohortEvidenceMetadata, ComparisonExtensionBuilder, ComparisonExtensionComparator,
    ComparisonExtensionDescriptor, ComparisonExtensionMetadata, ComparisonExtensionSelector,
};
pub use fixture::{ContractFixture, FixtureError};
pub use pipeline::{
    ChildActivityView, EventPipeline, EventPipelineBuilder, WindowPipelineBuilder, for_events,
};
pub use primitive::PrimitiveValue;
pub use records::{
    ClosedWindow, OpenWindow, WindowHistory, WindowHistoryFixture, WindowHistoryFixtureWindow,
    WindowRecordId, WindowSegment, WindowTag,
};
pub use temporal::{TemporalAxis, TemporalPoint, TemporalRange, TemporalRangeError};
