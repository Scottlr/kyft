#![forbid(unsafe_code)]
#![deny(missing_docs)]
//! Core Rust primitives for Spanfold.
//!
//! This crate is the start of Spanfold's Rust 1.95.0 / Rust 2024
//! implementation. It intentionally begins with strongly typed data structures
//! and builders that future comparison, export, and CLI work can use without a
//! mechanical translation from the .NET or Python codebases.

mod comparison;
mod fixture;
mod primitive;
mod records;
mod temporal;

pub use comparison::{
    AgainstSelection, Comparator, ComparatorSummary, ComparisonDiagnostic, ComparisonPlan,
    ComparisonResult, ComparisonRows, CoverageRow, MissingRow, OverlapRow, ResidualRow, RowRange,
    WindowFilter, compare,
};
pub use fixture::{ContractFixture, FixtureError};
pub use primitive::PrimitiveValue;
pub use records::{
    ClosedWindow, OpenWindow, WindowHistory, WindowHistoryFixture, WindowHistoryFixtureWindow,
    WindowRecordId, WindowSegment, WindowTag,
};
pub use temporal::{TemporalAxis, TemporalPoint, TemporalRange, TemporalRangeError};
