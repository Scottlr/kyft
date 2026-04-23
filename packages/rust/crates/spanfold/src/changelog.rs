use std::collections::BTreeMap;

use serde::Serialize;

use crate::{ComparisonFinality, ComparisonRowFinality};

/// Deterministic changelog entry between row-finality snapshots.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ComparisonChangelogEntry {
    /// Row family.
    #[serde(rename = "rowType")]
    pub row_type: String,
    /// Row identifier.
    #[serde(rename = "rowId")]
    pub row_id: String,
    /// Metadata version.
    pub version: u32,
    /// Finality transition emitted by the changelog.
    pub finality: ComparisonFinality,
    /// Prior row identifier superseded by this entry, when any.
    #[serde(rename = "supersedesRowId")]
    pub supersedes_row_id: Option<String>,
    /// Human-readable reason.
    pub reason: String,
}

/// Creates a changelog between row-finality snapshots.
#[must_use]
pub fn create_changelog(
    previous: &[ComparisonRowFinality],
    current: &[ComparisonRowFinality],
) -> Vec<ComparisonChangelogEntry> {
    let previous_by_key = previous
        .iter()
        .map(|row| ((row.row_type.clone(), row.row_id.clone()), row))
        .collect::<BTreeMap<_, _>>();
    let current_by_key = current
        .iter()
        .map(|row| ((row.row_type.clone(), row.row_id.clone()), row))
        .collect::<BTreeMap<_, _>>();
    let mut entries = Vec::new();

    for ((row_type, row_id), current_row) in &current_by_key {
        if let Some(previous_row) = previous_by_key.get(&(row_type.clone(), row_id.clone())) {
            if previous_row.finality == current_row.finality
                && previous_row.reason == current_row.reason
            {
                continue;
            }
            entries.push(ComparisonChangelogEntry {
                row_type: row_type.clone(),
                row_id: row_id.clone(),
                version: previous_row.version + 1,
                finality: ComparisonFinality::Revised,
                supersedes_row_id: Some(previous_row.row_id.clone()),
                reason: format!(
                    "Row metadata changed from {:?} to {:?}.",
                    previous_row.finality, current_row.finality
                ),
            });
            continue;
        }

        entries.push(ComparisonChangelogEntry {
            row_type: row_type.clone(),
            row_id: row_id.clone(),
            version: current_row.version,
            finality: current_row.finality.clone(),
            supersedes_row_id: current_row.supersedes_row_id.clone(),
            reason: current_row.reason.clone(),
        });
    }

    for ((row_type, row_id), previous_row) in &previous_by_key {
        if current_by_key.contains_key(&(row_type.clone(), row_id.clone())) {
            continue;
        }
        entries.push(ComparisonChangelogEntry {
            row_type: row_type.clone(),
            row_id: row_id.clone(),
            version: previous_row.version + 1,
            finality: ComparisonFinality::Retracted,
            supersedes_row_id: Some(previous_row.row_id.clone()),
            reason: "Row was not emitted by the current snapshot.".to_owned(),
        });
    }

    entries
}

/// Replays a changelog over a previous row-finality snapshot.
#[must_use]
pub fn replay_changelog(
    previous: &[ComparisonRowFinality],
    entries: &[ComparisonChangelogEntry],
) -> Vec<ComparisonRowFinality> {
    let mut active = previous
        .iter()
        .map(|row| ((row.row_type.clone(), row.row_id.clone()), row.clone()))
        .collect::<BTreeMap<_, _>>();

    for entry in entries {
        let key = (entry.row_type.clone(), entry.row_id.clone());
        if entry.finality == ComparisonFinality::Retracted {
            active.remove(&key);
            continue;
        }
        active.insert(
            key,
            ComparisonRowFinality {
                row_type: entry.row_type.clone(),
                row_id: entry.row_id.clone(),
                finality: if entry.finality == ComparisonFinality::Revised {
                    ComparisonFinality::Final
                } else {
                    entry.finality.clone()
                },
                reason: entry.reason.clone(),
                version: entry.version,
                supersedes_row_id: entry.supersedes_row_id.clone(),
            },
        );
    }

    active.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changelog_replays_revisions_and_retractions() {
        let previous = vec![ComparisonRowFinality {
            row_type: "residual".to_owned(),
            row_id: "residual[0]".to_owned(),
            finality: ComparisonFinality::Provisional,
            reason: "depends on an open window clipped to the evaluation horizon".to_owned(),
            version: 1,
            supersedes_row_id: None,
        }];
        let current = vec![ComparisonRowFinality {
            row_type: "residual".to_owned(),
            row_id: "residual[0]".to_owned(),
            finality: ComparisonFinality::Final,
            reason: "derived from closed windows".to_owned(),
            version: 1,
            supersedes_row_id: None,
        }];

        let entries = create_changelog(&previous, &current);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].finality, ComparisonFinality::Revised);

        let replayed = replay_changelog(&previous, &entries);
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].finality, ComparisonFinality::Final);
        assert_eq!(replayed[0].version, 2);
    }
}
