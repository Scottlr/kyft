use serde::{Deserialize, Serialize};

use crate::{PrimitiveValue, TemporalPoint, TemporalRange, TemporalRangeError};

/// Deterministic window record identifier.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct WindowRecordId(String);

impl WindowRecordId {
    /// Creates a new record identifier.
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the identifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Analytical segment captured with a window.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WindowSegment {
    /// Segment name.
    pub name: String,
    /// Segment value.
    pub value: PrimitiveValue,
    /// Optional parent segment name.
    pub parent_name: Option<String>,
}

impl WindowSegment {
    /// Creates a segment.
    #[must_use]
    pub fn new(name: impl Into<String>, value: impl Into<PrimitiveValue>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            parent_name: None,
        }
    }

    /// Sets the parent segment name.
    #[must_use]
    pub fn with_parent(mut self, parent_name: impl Into<String>) -> Self {
        self.parent_name = Some(parent_name.into());
        self
    }
}

/// Descriptive tag captured with a window.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WindowTag {
    /// Tag name.
    pub name: String,
    /// Tag value.
    pub value: PrimitiveValue,
}

impl WindowTag {
    /// Creates a tag.
    #[must_use]
    pub fn new(name: impl Into<String>, value: impl Into<PrimitiveValue>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

/// Closed state window.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClosedWindow {
    /// Window record ID.
    pub id: WindowRecordId,
    /// Window family name.
    pub window_name: String,
    /// Logical key.
    pub key: String,
    /// Window temporal range.
    pub range: TemporalRange,
    /// Availability point used for known-at filtering, when explicitly supplied.
    pub known_at: Option<TemporalPoint>,
    /// Optional source/lane.
    pub source: Option<String>,
    /// Optional partition.
    pub partition: Option<String>,
    /// Captured segments.
    pub segments: Vec<WindowSegment>,
    /// Captured tags.
    pub tags: Vec<WindowTag>,
}

/// Open state window.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct OpenWindow {
    /// Window record ID.
    pub id: WindowRecordId,
    /// Window family name.
    pub window_name: String,
    /// Logical key.
    pub key: String,
    /// Window start point.
    pub start: TemporalPoint,
    /// Availability point used for known-at filtering, when explicitly supplied.
    pub known_at: Option<TemporalPoint>,
    /// Optional source/lane.
    pub source: Option<String>,
    /// Optional partition.
    pub partition: Option<String>,
    /// Captured segments.
    pub segments: Vec<WindowSegment>,
    /// Captured tags.
    pub tags: Vec<WindowTag>,
}

/// In-memory history of open and closed windows.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WindowHistory {
    closed: Vec<ClosedWindow>,
    open: Vec<OpenWindow>,
}

impl WindowHistory {
    /// Creates an empty window history.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            closed: Vec::new(),
            open: Vec::new(),
        }
    }

    /// Returns closed windows.
    #[must_use]
    pub fn closed_windows(&self) -> &[ClosedWindow] {
        &self.closed
    }

    /// Returns open windows.
    #[must_use]
    pub fn open_windows(&self) -> &[OpenWindow] {
        &self.open
    }

    /// Builds a directional source matrix for one recorded window family.
    #[must_use]
    pub fn compare_sources(
        &self,
        name: &str,
        window_name: &str,
        sources: &[String],
    ) -> crate::SourceMatrixResult {
        crate::compare_sources(self, name, window_name, sources)
    }

    /// Compares parent and child window families as a hierarchy explanation.
    #[must_use]
    pub fn compare_hierarchy(
        &self,
        name: &str,
        parent_window_name: &str,
        child_window_name: &str,
    ) -> crate::HierarchyComparisonResult {
        crate::compare_hierarchy(self, name, parent_window_name, child_window_name)
    }

    pub(crate) fn push_closed(&mut self, window: ClosedWindow) {
        self.closed.push(window);
    }

    pub(crate) fn push_open(&mut self, window: OpenWindow) {
        self.open.push(window);
    }

    pub(crate) fn open_windows_mut(&mut self) -> &mut Vec<OpenWindow> {
        &mut self.open
    }
}

/// Fixture-oriented builder for compact histories.
#[derive(Clone, Debug, Default)]
pub struct WindowHistoryFixture {
    history: WindowHistory,
    next_record_id: u64,
}

impl WindowHistoryFixture {
    /// Creates a fixture builder.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            history: WindowHistory::new(),
            next_record_id: 0,
        }
    }

    /// Adds a closed processing-position window.
    pub fn closed_window(
        mut self,
        window_name: impl Into<String>,
        key: impl Into<String>,
        start_position: i64,
        end_position: i64,
        configure: impl FnOnce(WindowHistoryFixtureWindow) -> WindowHistoryFixtureWindow,
    ) -> Result<Self, TemporalRangeError> {
        let metadata = configure(WindowHistoryFixtureWindow::default());
        let id = self.next_id();
        let range = TemporalRange::positions(start_position, end_position)?;
        self.history.push_closed(ClosedWindow {
            id,
            window_name: window_name.into(),
            key: key.into(),
            range,
            known_at: metadata.known_at,
            source: metadata.source,
            partition: metadata.partition,
            segments: metadata.segments,
            tags: metadata.tags,
        });
        Ok(self)
    }

    /// Adds an open processing-position window.
    #[must_use]
    pub fn open_window(
        mut self,
        window_name: impl Into<String>,
        key: impl Into<String>,
        start_position: i64,
        configure: impl FnOnce(WindowHistoryFixtureWindow) -> WindowHistoryFixtureWindow,
    ) -> Self {
        let metadata = configure(WindowHistoryFixtureWindow::default());
        let id = self.next_id();
        self.history.push_open(OpenWindow {
            id,
            window_name: window_name.into(),
            key: key.into(),
            start: TemporalPoint::position(start_position),
            known_at: metadata.known_at,
            source: metadata.source,
            partition: metadata.partition,
            segments: metadata.segments,
            tags: metadata.tags,
        });
        self
    }

    /// Builds the history.
    #[must_use]
    pub fn build(self) -> WindowHistory {
        self.history
    }

    fn next_id(&mut self) -> WindowRecordId {
        let id = WindowRecordId::new(format!("window-{:04}", self.next_record_id));
        self.next_record_id += 1;
        id
    }
}

/// Metadata builder for one fixture window.
#[derive(Clone, Debug, Default)]
pub struct WindowHistoryFixtureWindow {
    known_at: Option<TemporalPoint>,
    source: Option<String>,
    partition: Option<String>,
    segments: Vec<WindowSegment>,
    tags: Vec<WindowTag>,
}

impl WindowHistoryFixtureWindow {
    /// Sets the source/lane.
    #[must_use]
    pub fn source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }

    /// Sets the known-at processing position for the window.
    #[must_use]
    pub fn known_at_position(mut self, position: i64) -> Self {
        self.known_at = Some(TemporalPoint::position(position));
        self
    }

    /// Sets the partition.
    #[must_use]
    pub fn partition(mut self, partition: impl Into<String>) -> Self {
        self.partition = Some(partition.into());
        self
    }

    /// Adds a segment.
    #[must_use]
    pub fn segment(mut self, name: impl Into<String>, value: impl Into<PrimitiveValue>) -> Self {
        self.segments.push(WindowSegment::new(name, value));
        self
    }

    /// Adds a segment with parent metadata.
    #[must_use]
    pub fn child_segment(
        mut self,
        name: impl Into<String>,
        value: impl Into<PrimitiveValue>,
        parent_name: impl Into<String>,
    ) -> Self {
        self.segments
            .push(WindowSegment::new(name, value).with_parent(parent_name));
        self
    }

    /// Adds a tag.
    #[must_use]
    pub fn tag(mut self, name: impl Into<String>, value: impl Into<PrimitiveValue>) -> Self {
        self.tags.push(WindowTag::new(name, value));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixture_builder_creates_closed_windows_with_metadata() {
        let history = WindowHistoryFixture::new()
            .closed_window("DeviceOffline", "device-1", 1, 5, |w| {
                w.source("provider-a")
                    .partition("fleet-a")
                    .segment("lifecycle", "Incident")
                    .child_segment("stage", "Escalated", "lifecycle")
                    .tag("fleet", "critical")
            })
            .expect("valid fixture window")
            .build();

        let window = &history.closed_windows()[0];
        assert_eq!(window.id.as_str(), "window-0000");
        assert_eq!(window.source.as_deref(), Some("provider-a"));
        assert_eq!(window.partition.as_deref(), Some("fleet-a"));
        assert_eq!(window.segments.len(), 2);
        assert_eq!(window.tags.len(), 1);
        assert_eq!(window.range.magnitude(), 4);
    }

    #[test]
    fn fixture_builder_creates_open_windows() {
        let history = WindowHistoryFixture::new()
            .open_window("DeviceOffline", "device-1", 10, |w| w.source("provider-a"))
            .build();

        assert_eq!(history.open_windows().len(), 1);
        assert_eq!(history.open_windows()[0].start, TemporalPoint::position(10));
    }
}
