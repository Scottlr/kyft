use std::{collections::BTreeMap, marker::PhantomData, sync::Arc};

use crate::{
    ClosedWindow, OpenWindow, TemporalPoint, TemporalRange, WindowHistory, WindowRecordId,
};

type KeySelector<T> = Arc<dyn Fn(&T) -> String + Send + Sync + 'static>;
type ActivePredicate<T> = Arc<dyn Fn(&T) -> bool + Send + Sync + 'static>;
type RollupPredicate = Arc<dyn Fn(ChildActivityView) -> bool + Send + Sync + 'static>;

struct RollUpDefinition<T> {
    name: String,
    key: KeySelector<T>,
    is_active: RollupPredicate,
    rollups: Vec<RollUpDefinition<T>>,
}

struct WindowDefinition<T> {
    name: String,
    key: KeySelector<T>,
    is_active: ActivePredicate<T>,
    rollups: Vec<RollUpDefinition<T>>,
}

impl<T> Clone for RollUpDefinition<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            key: Arc::clone(&self.key),
            is_active: Arc::clone(&self.is_active),
            rollups: self.rollups.clone(),
        }
    }
}

impl<T> Clone for WindowDefinition<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            key: Arc::clone(&self.key),
            is_active: Arc::clone(&self.is_active),
            rollups: self.rollups.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OpenState {
    start_position: i64,
}

#[derive(Clone, Debug, Default)]
struct ParentState {
    children: BTreeMap<String, bool>,
}

#[derive(Clone, Copy)]
struct ChildContext<'a> {
    lineage: &'a str,
    key: &'a str,
    is_active: bool,
}

impl ParentState {
    fn view(&self) -> ChildActivityView {
        ChildActivityView {
            active_count: self.children.values().filter(|active| **active).count(),
            total_count: self.children.len(),
        }
    }
}

/// Snapshot of known child activity for a roll-up parent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChildActivityView {
    /// Number of active children.
    pub active_count: usize,
    /// Number of known children.
    pub total_count: usize,
}

impl ChildActivityView {
    /// Returns whether every known child is active.
    #[must_use]
    pub fn all_active(self) -> bool {
        self.total_count > 0 && self.active_count == self.total_count
    }

    /// Returns whether at least one known child is active.
    #[must_use]
    pub fn any_active(self) -> bool {
        self.active_count > 0
    }
}

/// Builder for an event ingestion pipeline.
#[derive(Clone)]
pub struct EventPipelineBuilder<T> {
    windows: Vec<WindowDefinition<T>>,
    marker: PhantomData<T>,
}

impl<T> Default for EventPipelineBuilder<T> {
    fn default() -> Self {
        Self {
            windows: Vec::new(),
            marker: PhantomData,
        }
    }
}

/// Builder returned while configuring one source window and its roll-ups.
pub struct WindowPipelineBuilder<T> {
    builder: EventPipelineBuilder<T>,
    path: Vec<usize>,
}

/// Event ingestion pipeline that records source windows and roll-ups.
pub struct EventPipeline<T> {
    windows: Vec<WindowDefinition<T>>,
    history: WindowHistory,
    active: BTreeMap<(String, String, Option<String>, Option<String>), OpenState>,
    parents: BTreeMap<(String, String, Option<String>, Option<String>), ParentState>,
    position: i64,
    next_record_id: u64,
    marker: PhantomData<T>,
}

/// Creates a new event pipeline builder for one event type.
#[must_use]
pub fn for_events<T>() -> EventPipelineBuilder<T> {
    EventPipelineBuilder::default()
}

impl<T> EventPipelineBuilder<T> {
    /// Starts recording windows for the configured event type.
    #[must_use]
    pub fn record_windows(self) -> Self {
        self
    }

    /// Adds a tracked window and returns the builder.
    #[must_use]
    pub fn track_window<K, F, P>(mut self, name: impl Into<String>, key: F, is_active: P) -> Self
    where
        K: Into<String> + 'static,
        F: Fn(&T) -> K + Send + Sync + 'static,
        P: Fn(&T) -> bool + Send + Sync + 'static,
    {
        self.windows.push(WindowDefinition {
            name: name.into(),
            key: Arc::new(move |event| key(event).into()),
            is_active: Arc::new(is_active),
            rollups: Vec::new(),
        });
        self
    }

    /// Adds a tracked window and returns a nested roll-up builder.
    #[must_use]
    pub fn window<K, F, P>(
        mut self,
        name: impl Into<String>,
        key: F,
        is_active: P,
    ) -> WindowPipelineBuilder<T>
    where
        K: Into<String> + 'static,
        F: Fn(&T) -> K + Send + Sync + 'static,
        P: Fn(&T) -> bool + Send + Sync + 'static,
    {
        self.windows.push(WindowDefinition {
            name: name.into(),
            key: Arc::new(move |event| key(event).into()),
            is_active: Arc::new(is_active),
            rollups: Vec::new(),
        });
        WindowPipelineBuilder {
            path: vec![self.windows.len() - 1],
            builder: self,
        }
    }

    /// Builds the pipeline.
    #[must_use]
    pub fn build(self) -> EventPipeline<T> {
        EventPipeline {
            windows: self.windows,
            history: WindowHistory::new(),
            active: BTreeMap::new(),
            parents: BTreeMap::new(),
            position: 0,
            next_record_id: 0,
            marker: PhantomData,
        }
    }
}

impl<T> WindowPipelineBuilder<T> {
    /// Adds a nested roll-up to the current window or roll-up node.
    #[must_use]
    pub fn roll_up<K, F, P>(mut self, name: impl Into<String>, key: F, is_active: P) -> Self
    where
        K: Into<String> + 'static,
        F: Fn(&T) -> K + Send + Sync + 'static,
        P: Fn(ChildActivityView) -> bool + Send + Sync + 'static,
    {
        let definition = RollUpDefinition {
            name: name.into(),
            key: Arc::new(move |event| key(event).into()),
            is_active: Arc::new(is_active),
            rollups: Vec::new(),
        };
        let next_index = add_rollup(&mut self.builder.windows, &self.path, definition);
        self.path.push(next_index);
        self
    }

    /// Builds the pipeline.
    #[must_use]
    pub fn build(self) -> EventPipeline<T> {
        self.builder.build()
    }
}

impl<T> EventPipeline<T> {
    /// Returns the latest processing position.
    #[must_use]
    pub fn processing_position(&self) -> i64 {
        self.position
    }

    /// Returns the recorded window history.
    #[must_use]
    pub fn history(&self) -> &WindowHistory {
        &self.history
    }

    /// Ingests one event with optional source and partition context.
    pub fn ingest(&mut self, event: T, source: Option<&str>, partition: Option<&str>) {
        self.position += 1;
        for definition in self.windows.clone() {
            self.ingest_definition(&definition, &event, source, partition);
        }
    }

    fn ingest_definition(
        &mut self,
        definition: &WindowDefinition<T>,
        event: &T,
        source: Option<&str>,
        partition: Option<&str>,
    ) {
        let key = (definition.key)(event);
        let is_active = (definition.is_active)(event);
        self.sync_window_state(&definition.name, &key, source, partition, is_active);

        for rollup in &definition.rollups {
            self.sync_rollup(
                rollup,
                event,
                source,
                partition,
                ChildContext {
                    lineage: &definition.name,
                    key: &key,
                    is_active,
                },
            );
        }
    }

    fn sync_rollup(
        &mut self,
        definition: &RollUpDefinition<T>,
        event: &T,
        source: Option<&str>,
        partition: Option<&str>,
        child: ChildContext<'_>,
    ) {
        let key = (definition.key)(event);
        let state_key = (
            format!("{}>{}", child.lineage, definition.name),
            key.clone(),
            source.map(str::to_owned),
            partition.map(str::to_owned),
        );
        let parent_state = self.parents.entry(state_key).or_default();
        parent_state
            .children
            .insert(child.key.to_owned(), child.is_active);
        let is_active = (definition.is_active)(parent_state.view());
        self.sync_window_state(&definition.name, &key, source, partition, is_active);

        for rollup in &definition.rollups {
            self.sync_rollup(
                rollup,
                event,
                source,
                partition,
                ChildContext {
                    lineage: &definition.name,
                    key: &key,
                    is_active,
                },
            );
        }
    }

    fn sync_window_state(
        &mut self,
        window_name: &str,
        key: &str,
        source: Option<&str>,
        partition: Option<&str>,
        is_active: bool,
    ) {
        let state_key = (
            window_name.to_owned(),
            key.to_owned(),
            source.map(str::to_owned),
            partition.map(str::to_owned),
        );

        if is_active {
            if self.active.contains_key(&state_key) {
                return;
            }
            let id = self.next_id();
            self.history.push_open(OpenWindow {
                id: id.clone(),
                window_name: window_name.to_owned(),
                key: key.to_owned(),
                start: TemporalPoint::position(self.position),
                known_at: None,
                source: source.map(str::to_owned),
                partition: partition.map(str::to_owned),
                segments: Vec::new(),
                tags: Vec::new(),
            });
            self.active.insert(
                state_key,
                OpenState {
                    start_position: self.position,
                },
            );
            return;
        }

        let Some(open_state) = self.active.remove(&state_key) else {
            return;
        };
        let open_windows = self.history.open_windows_mut();
        let Some(index) = open_windows.iter().position(|window| {
            window.window_name == window_name
                && window.key == key
                && window.source.as_deref() == source
                && window.partition.as_deref() == partition
        }) else {
            return;
        };
        let open = open_windows.remove(index);
        self.history.push_closed(ClosedWindow {
            id: open.id,
            window_name: open.window_name,
            key: open.key,
            range: TemporalRange::positions(open_state.start_position, self.position)
                .expect("valid processing-position range"),
            known_at: open.known_at,
            source: open.source,
            partition: open.partition,
            segments: open.segments,
            tags: open.tags,
        });
    }

    fn next_id(&mut self) -> WindowRecordId {
        let id = WindowRecordId::new(format!("pipeline-{:04}", self.next_record_id));
        self.next_record_id += 1;
        id
    }
}

fn add_rollup<T>(
    windows: &mut [WindowDefinition<T>],
    path: &[usize],
    definition: RollUpDefinition<T>,
) -> usize {
    let mut rollups = &mut windows[path[0]].rollups;
    for index in &path[1..] {
        rollups = &mut rollups[*index].rollups;
    }
    rollups.push(definition);
    rollups.len() - 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct PriceTick {
        selection_id: &'static str,
        market_id: &'static str,
        fixture_id: &'static str,
        price: f64,
    }

    #[test]
    fn track_window_records_closed_history() {
        let mut pipeline = for_events::<PriceTick>()
            .record_windows()
            .track_window(
                "SelectionSuspension",
                |tick| tick.selection_id,
                |tick| tick.price == 0.0,
            )
            .build();

        pipeline.ingest(
            PriceTick {
                selection_id: "selection-1",
                market_id: "market-1",
                fixture_id: "fixture-1",
                price: 0.0,
            },
            Some("provider-a"),
            None,
        );
        pipeline.ingest(
            PriceTick {
                selection_id: "selection-1",
                market_id: "market-1",
                fixture_id: "fixture-1",
                price: 1.2,
            },
            Some("provider-a"),
            None,
        );

        assert_eq!(pipeline.history().closed_windows().len(), 1);
        assert_eq!(
            pipeline.history().closed_windows()[0].window_name,
            "SelectionSuspension"
        );
    }

    #[test]
    fn nested_rollups_record_parent_windows() {
        let mut pipeline = for_events::<PriceTick>()
            .record_windows()
            .window(
                "SelectionSuspension",
                |tick| tick.selection_id,
                |tick| tick.price == 0.0,
            )
            .roll_up(
                "MarketSuspension",
                |tick| tick.market_id,
                |children| children.any_active(),
            )
            .roll_up(
                "FixtureSuspension",
                |tick| tick.fixture_id,
                |children| children.any_active(),
            )
            .build();

        pipeline.ingest(
            PriceTick {
                selection_id: "selection-1",
                market_id: "market-1",
                fixture_id: "fixture-1",
                price: 0.0,
            },
            None,
            None,
        );
        pipeline.ingest(
            PriceTick {
                selection_id: "selection-1",
                market_id: "market-1",
                fixture_id: "fixture-1",
                price: 1.1,
            },
            None,
            None,
        );

        let history = pipeline.history();
        assert_eq!(history.closed_windows().len(), 3);
        let hierarchy = history.compare_hierarchy(
            "Market explanation",
            "MarketSuspension",
            "SelectionSuspension",
        );
        assert_eq!(hierarchy.rows.len(), 1);
        assert_eq!(
            hierarchy.rows[0].kind,
            crate::HierarchyComparisonRowKind::ParentExplained
        );
    }
}
