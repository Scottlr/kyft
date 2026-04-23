use serde::Serialize;

/// Selector descriptor exposed by a comparison extension.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ComparisonExtensionSelector {
    /// Stable selector name.
    pub name: String,
    /// Human-readable description.
    pub description: String,
}

/// Comparator descriptor exposed by a comparison extension.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ComparisonExtensionComparator {
    /// Stable comparator declaration.
    pub declaration: String,
    /// Human-readable description.
    pub description: String,
}

/// Immutable comparison extension descriptor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ComparisonExtensionDescriptor {
    /// Stable extension identifier.
    pub id: String,
    /// Human-readable display name.
    #[serde(rename = "displayName")]
    pub display_name: String,
    /// Exposed selectors.
    pub selectors: Vec<ComparisonExtensionSelector>,
    /// Exposed comparators.
    pub comparators: Vec<ComparisonExtensionComparator>,
    /// Exposed metadata keys.
    #[serde(rename = "metadataKeys")]
    pub metadata_keys: Vec<String>,
}

/// Serializable metadata emitted by a comparison extension.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ComparisonExtensionMetadata {
    /// Stable extension identifier.
    #[serde(rename = "extensionId")]
    pub extension_id: String,
    /// Stable metadata key.
    pub key: String,
    /// Serialized metadata payload.
    pub value: String,
}

/// Parsed evidence for one cohort-aligned segment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct CohortEvidenceMetadata {
    /// Aligned segment index that emitted the metadata.
    #[serde(rename = "segmentIndex")]
    pub segment_index: usize,
    /// Cohort activity rule.
    pub rule: String,
    /// Required active-member count.
    #[serde(rename = "requiredCount")]
    pub required_count: usize,
    /// Observed active-member count.
    #[serde(rename = "activeCount")]
    pub active_count: usize,
    /// Whether the cohort lane was active.
    #[serde(rename = "isActive")]
    pub is_active: bool,
    /// Active source identities.
    #[serde(rename = "activeSources")]
    pub active_sources: Vec<String>,
    /// Raw metadata payload.
    #[serde(rename = "rawValue")]
    pub raw_value: String,
}

/// Builder for immutable comparison extension descriptors.
#[derive(Clone, Debug)]
pub struct ComparisonExtensionBuilder {
    id: String,
    display_name: String,
    selectors: Vec<ComparisonExtensionSelector>,
    comparators: Vec<ComparisonExtensionComparator>,
    metadata_keys: Vec<String>,
}

impl ComparisonExtensionBuilder {
    /// Creates a builder for one extension descriptor.
    #[must_use]
    pub fn new(id: impl Into<String>, display_name: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            display_name: display_name.into(),
            selectors: Vec::new(),
            comparators: Vec::new(),
            metadata_keys: Vec::new(),
        }
    }

    /// Registers a selector descriptor.
    #[must_use]
    pub fn add_selector(mut self, name: impl Into<String>, description: impl Into<String>) -> Self {
        self.selectors.push(ComparisonExtensionSelector {
            name: name.into(),
            description: description.into(),
        });
        self
    }

    /// Registers a comparator descriptor.
    #[must_use]
    pub fn add_comparator(
        mut self,
        declaration: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        self.comparators.push(ComparisonExtensionComparator {
            declaration: declaration.into(),
            description: description.into(),
        });
        self
    }

    /// Registers a metadata key.
    #[must_use]
    pub fn add_metadata_key(mut self, key: impl Into<String>) -> Self {
        self.metadata_keys.push(key.into());
        self
    }

    /// Builds the immutable descriptor.
    #[must_use]
    pub fn build(self) -> ComparisonExtensionDescriptor {
        ComparisonExtensionDescriptor {
            id: self.id,
            display_name: self.display_name,
            selectors: self.selectors,
            comparators: self.comparators,
            metadata_keys: self.metadata_keys,
        }
    }
}
