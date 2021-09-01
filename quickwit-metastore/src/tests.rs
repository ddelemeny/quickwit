/*
    Quickwit
    Copyright (C) 2021 Quickwit Inc.

    Quickwit is offered under the AGPL v3.0 and as commercial software.
    For commercial licensing, contact us at hello@quickwit.io.

    AGPL:
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

use std::collections::HashSet;
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use tokio::time::{sleep, Duration};

use crate::checkpoint::{Checkpoint, CheckpointDelta};
use crate::{
    IndexMetadata, Metastore, MetastoreError, SplitMetadata, SplitMetadataAndFooterOffsets,
    SplitState,
};

#[cfg(test)]
#[async_trait]
pub trait DefaultForTest {
    async fn default_for_test() -> Self;
}

fn to_set(tags: &[&str]) -> HashSet<String> {
    tags.iter().map(ToString::to_string).collect()
}

#[allow(dead_code)]
pub async fn test_metastore_create_index<MetastoreToTest: Metastore + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let index_id = "create-index-my-index";
    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: "ram://indexes/my-index".to_string(),
        index_config: Arc::new(quickwit_index_config::default_config_for_tests()),
        checkpoint: Checkpoint::default(),
    };

    // Create an index
    let result = metastore
        .create_index(index_metadata.clone())
        .await
        .unwrap();
    assert!(matches!(result, ()));

    // Create an index that already exists
    let result = metastore
        .create_index(index_metadata.clone())
        .await
        .unwrap_err();
    assert!(matches!(result, MetastoreError::IndexAlreadyExists { .. }));
    // Delete an index
    let result = metastore.delete_index(index_id).await.unwrap();
    assert!(matches!(result, ()));
}

#[cfg(test)]
#[allow(dead_code)]
pub async fn test_metastore_delete_index<MetastoreToTest: Metastore + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let index_id = "delte-index-my-index";
    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: "ram://indexes/my-index".to_string(),
        index_config: Arc::new(quickwit_index_config::default_config_for_tests()),
        checkpoint: Checkpoint::default(),
    };

    // Delete a non-existent index
    let result = metastore
        .delete_index("non-existent-index")
        .await
        .unwrap_err();
    assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));

    // Create an index
    let result = metastore
        .create_index(index_metadata.clone())
        .await
        .unwrap();
    assert!(matches!(result, ()));

    // Delete an index
    let result = metastore.delete_index(index_id).await.unwrap();
    assert!(matches!(result, ()));
}

#[cfg(test)]
#[allow(unused_variables)]
#[allow(dead_code)]
pub async fn test_metastore_index_metadata<MetastoreToTest: Metastore + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let index_id = "index-metadata-my-index";
    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: "ram://indexes/my-index".to_string(),
        index_config: Arc::new(quickwit_index_config::default_config_for_tests()),
        checkpoint: Checkpoint::default(),
    };

    // Get a non-existent index metadata
    let result = metastore
        .index_metadata("non-existent-index")
        .await
        .unwrap_err();
    assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));

    // Create an index
    let result = metastore
        .create_index(index_metadata.clone())
        .await
        .unwrap();
    assert!(matches!(result, ()));

    // Get an index metadata
    let result = metastore.index_metadata(index_id).await.unwrap();
    assert!(matches!(result, index_metadata));
}

#[allow(dead_code)]
pub async fn test_metastore_stage_split<MetastoreToTest: Metastore + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let current_timestamp = Utc::now().timestamp();

    let index_id = "my-index";
    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: "ram://indexes/my-index".to_string(),
        index_config: Arc::new(AllFlattenIndexConfig::default()),
        checkpoint: Checkpoint::default(),
    };

    let split_id = "one";
    let split_metadata = SplitMetadataAndFooterOffsets {
        split_metadata: SplitMetadata {
            split_id: split_id.to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            generation: 3,
            update_timestamp: current_timestamp,
            ..Default::default()
        },
        footer_offsets: 1000..2000,
    };

    // Stage a split on a non-existent index
    let result = metastore
        .stage_split("non-existent-index", split_metadata.clone())
        .await
        .unwrap_err();
    assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));

    // Create an index
    let result = metastore
        .create_index(index_metadata.clone())
        .await
        .unwrap();
    assert!(matches!(result, ()));

    // Stage a split on an index
    let result = metastore
        .stage_split(index_id, split_metadata.clone())
        .await
        .unwrap();
    assert!(matches!(result, ()));

    // Stage a existent-split on an index
    let result = metastore
        .stage_split(index_id, split_metadata.clone())
        .await
        .unwrap_err();
    assert!(matches!(result, MetastoreError::InternalError { .. }));
}

#[allow(dead_code)]
pub async fn test_metastore_publish_splits<MetastoreToTest: Metastore + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let current_timestamp = Utc::now().timestamp();

    let index_id = "my-index";
    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: "ram://indexes/my-index".to_string(),
        index_config: Arc::new(AllFlattenIndexConfig::default()),
        checkpoint: Checkpoint::default(),
    };

    let split_id_1 = "one";
    let split_metadata_1 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: split_id_1.to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            generation: 3,
            update_timestamp: current_timestamp,
            ..Default::default()
        },
    };

    let split_id_2 = "two";
    let split_metadata_2 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: split_id_2.to_string(),
            split_state: SplitState::Staged,
            num_records: 5,
            size_in_bytes: 6,
            time_range: Some(RangeInclusive::new(30, 99)),
            generation: 2,
            update_timestamp: current_timestamp,
            ..Default::default()
        },
    };

    // Publish a split on a non-existent index
    {
        let result = metastore
            .publish_splits(
                "non-existent-index",
                &["non-existent-split"],
                CheckpointDelta::from(1..10),
            )
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
    }

    // Publish a non-existent split on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(
                index_id,
                &["non-existent-split"],
                CheckpointDelta::default(),
            )
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::SplitDoesNotExist { .. }));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Publish a staged split on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(index_id, &[split_id_1], CheckpointDelta::default())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Publish a published split on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(index_id, &[split_id_1], CheckpointDelta::from(1..12))
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let publish_error = metastore
            .publish_splits(index_id, &[split_id_1], CheckpointDelta::from(1..12))
            .await
            .unwrap_err();
        assert!(matches!(
            publish_error,
            MetastoreError::IncompatibleCheckpointDelta(_)
        ));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Publish a non-staged split on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(index_id, &[split_id_1], CheckpointDelta::from(12..15))
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .mark_splits_as_deleted(index_id, &[split_id_1])
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(index_id, &[split_id_1], CheckpointDelta::from(15..18))
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::SplitIsNotStaged { .. }));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Publish a staged split and non-existent split on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(
                index_id,
                &[split_id_1, "non-existent-split"],
                CheckpointDelta::from(15..18),
            )
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::SplitDoesNotExist { .. }));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Publish a published split and non-existant split on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(index_id, &[split_id_1], CheckpointDelta::from(15..18))
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(
                index_id,
                &[split_id_1, "non-existent-split"],
                CheckpointDelta::from(18..24),
            )
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::SplitDoesNotExist { .. }));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Publish a non-staged split and non-existant split on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(index_id, &[split_id_1], CheckpointDelta::from(18..24))
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .mark_splits_as_deleted(index_id, &[split_id_1])
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(
                index_id,
                &[split_id_1, "non-existent-split"],
                CheckpointDelta::from(24..26),
            )
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::SplitIsNotStaged { .. }));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Publish staged splits on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_2.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(
                index_id,
                &[split_id_1, split_id_2],
                CheckpointDelta::from(24..26),
            )
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Publish a staged split and published split on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_2.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(index_id, &[split_id_2], CheckpointDelta::from(26..28))
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(
                index_id,
                &[split_id_1, split_id_2],
                CheckpointDelta::from(28..30),
            )
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Publish published splits on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_2.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(
                index_id,
                &[split_id_1, split_id_2],
                CheckpointDelta::from(30..31),
            )
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let publish_error = metastore
            .publish_splits(
                index_id,
                &[split_id_1, split_id_2],
                CheckpointDelta::from(30..31),
            )
            .await
            .unwrap_err();
        assert!(matches!(
            publish_error,
            MetastoreError::IncompatibleCheckpointDelta(_)
        ));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }
}

#[allow(dead_code)]
pub async fn test_metastore_replace_splits<MetastoreToTest: Metastore + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let current_timestamp = Utc::now().timestamp();

    let index_id = "my-index";
    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: "ram://indexes/my-index".to_string(),
        index_config: Arc::new(AllFlattenIndexConfig::default()),
        checkpoint: Checkpoint::default(),
    };

    let split_id_1 = "one";
    let split_metadata_1 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: split_id_1.to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: None,
            generation: 1,
            update_timestamp: current_timestamp,
            ..Default::default()
        },
    };

    let split_id_2 = "two";
    let split_metadata_2 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: split_id_2.to_string(),
            split_state: SplitState::Staged,
            num_records: 5,
            size_in_bytes: 6,
            time_range: None,
            generation: 1,
            update_timestamp: current_timestamp,
            ..Default::default()
        },
    };

    let split_id_3 = "three";
    let split_metadata_3 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: split_id_3.to_string(),
            split_state: SplitState::Staged,
            num_records: 5,
            size_in_bytes: 6,
            time_range: None,
            generation: 1,
            update_timestamp: current_timestamp,
            ..Default::default()
        },
    };

    // Replace splits on a non-existent index
    {
        let result = metastore
            .replace_splits(
                "non-existent-index",
                &["non-existent-split-one"],
                &["non-existent-split-two"],
            )
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
    }

    // Replace a non-existent split on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .replace_splits(
                index_id,
                &["non-existent-split"],
                &["non-existent-split-two"],
            )
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::SplitDoesNotExist { .. }));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Replace a publish split with a non existing split
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(index_id, &[split_id_1], CheckpointDelta::default())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .replace_splits(index_id, &[split_id_2], &[split_id_1])
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::SplitDoesNotExist { .. }));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Replace a publish split with a deleted split
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));
        let result = metastore
            .stage_split(index_id, split_metadata_2.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(
                index_id,
                &[split_id_1, split_id_2],
                CheckpointDelta::default(),
            )
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .mark_splits_as_deleted(index_id, &[split_id_2])
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .replace_splits(index_id, &[split_id_2], &[split_id_1])
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::SplitIsNotStaged { .. }));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Replace a publish split with mixed splits
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(index_id, &[split_id_1], CheckpointDelta::default())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_2.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .replace_splits(index_id, &[split_id_2, split_id_3], &[split_id_1])
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::SplitDoesNotExist { .. }));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Replace a publish split with staged splits
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(index_id, &[split_id_1], CheckpointDelta::default())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_2.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));
        let result = metastore
            .stage_split(index_id, split_metadata_3.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .replace_splits(index_id, &[split_id_2, split_id_3], &[split_id_1])
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }
}

#[allow(dead_code)]
pub async fn test_metastore_mark_splits_as_deleted<MetastoreToTest: Metastore + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let current_timestamp = Utc::now().timestamp();

    let index_id = "my-index";
    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: "ram://indexes/my-index".to_string(),
        index_config: Arc::new(AllFlattenIndexConfig::default()),
        checkpoint: Checkpoint::default(),
    };

    let split_id_1 = "one";
    let split_metadata_1 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: split_id_1.to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            generation: 3,
            update_timestamp: current_timestamp,
            ..Default::default()
        },
    };

    // Mark a split as deleted on a non-existent index
    {
        let result = metastore
            .mark_splits_as_deleted("non-existent-index", &["non-existent-split"])
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
    }

    // Mark a non-existent split as deleted on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .mark_splits_as_deleted(index_id, &["non-existent-split"])
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::SplitDoesNotExist { .. }));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Mark a existent split as deleted on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .mark_splits_as_deleted(index_id, &[split_id_1])
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }
}

#[allow(dead_code)]
pub async fn test_metastore_delete_splits<MetastoreToTest: Metastore + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let current_timestamp = Utc::now().timestamp();

    let index_id = "my-index";
    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: "ram://indexes/my-index".to_string(),
        index_config: Arc::new(AllFlattenIndexConfig::default()),
        checkpoint: Checkpoint::default(),
    };

    let split_id_1 = "one";
    let split_metadata_1 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: split_id_1.to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            generation: 3,
            update_timestamp: current_timestamp,
            ..Default::default()
        },
    };

    // Delete a split as deleted on a non-existent index
    {
        let result = metastore
            .delete_splits("non-existent-index", &["non-existent-split"])
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
    }

    // Delete a non-existent split as deleted on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .delete_splits(index_id, &["non-existent-split"])
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::SplitDoesNotExist { .. }));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Delete a staged split on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .delete_splits(index_id, &[split_id_1])
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Delete a split that has been marked as deleted on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .mark_splits_as_deleted(index_id, &[split_id_1])
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .delete_splits(index_id, &[split_id_1])
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    // Delete a split that is not marked as deleted
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .publish_splits(index_id, &[split_id_1], CheckpointDelta::from(0..5))
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .delete_splits(index_id, &[split_id_1])
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::Forbidden { .. }));

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }
}

#[allow(dead_code)]
pub async fn test_metastore_list_all_splits<MetastoreToTest: Metastore + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let current_timestamp = Utc::now().timestamp();

    let index_id = "my-index";
    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: "ram://indexes/my-index".to_string(),
        index_config: Arc::new(AllFlattenIndexConfig::default()),
        checkpoint: Checkpoint::default(),
    };

    let split_id_1 = "one";
    let split_metadata_1 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: split_id_1.to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            generation: 3,
            update_timestamp: current_timestamp,
            ..Default::default()
        },
    };

    let split_metadata_2 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: "two".to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(100, 199)),
            generation: 3,
            update_timestamp: current_timestamp,
            ..Default::default()
        },
    };

    let split_metadata_3 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: "three".to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(200, 299)),
            generation: 3,
            update_timestamp: current_timestamp,
            ..Default::default()
        },
    };

    let split_metadata_4 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: "four".to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(300, 399)),
            generation: 3,
            update_timestamp: current_timestamp,
            ..Default::default()
        },
    };

    let split_metadata_5 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: "five".to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: None,
            generation: 3,
            update_timestamp: current_timestamp,
            ..Default::default()
        },
    };

    // List all splits on a non-existent index
    {
        let result = metastore
            .list_all_splits("non-existent-index")
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
    }

    // List all splits on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_2.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_3.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_4.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_5.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let splits = metastore.list_all_splits(index_id).await.unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), true);
        assert_eq!(split_ids.contains("two"), true);
        assert_eq!(split_ids.contains("three"), true);
        assert_eq!(split_ids.contains("four"), true);
        assert_eq!(split_ids.contains("five"), true);

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }
}

#[allow(dead_code)]
pub async fn test_metastore_list_splits<MetastoreToTest: Metastore + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let current_timestamp = Utc::now().timestamp();

    let index_id = "my-index";
    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: "ram://indexes/my-index".to_string(),
        index_config: Arc::new(AllFlattenIndexConfig::default()),
        checkpoint: Checkpoint::default(),
    };

    let split_id_1 = "one";
    let split_metadata_1 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: split_id_1.to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            generation: 3,
            update_timestamp: current_timestamp,
            tags: to_set(&["foo", "bar"]),
        },
    };

    let split_metadata_2 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: "two".to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(100, 199)),
            generation: 3,
            update_timestamp: current_timestamp,
            tags: to_set(&["bar"]),
        },
    };

    let split_metadata_3 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: "three".to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(200, 299)),
            generation: 3,
            update_timestamp: current_timestamp,
            tags: to_set(&["foo", "baz"]),
        },
    };

    let split_metadata_4 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: "four".to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(300, 399)),
            generation: 3,
            update_timestamp: current_timestamp,
            tags: to_set(&["foo"]),
        },
    };

    let split_metadata_5 = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: "five".to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: None,
            generation: 3,
            update_timestamp: current_timestamp,
            tags: to_set(&["baz", "biz"]),
        },
    };

    // List all splits on a non-existent index
    {
        let result = metastore
            .list_splits("non-existent-index", SplitState::Staged, None, &[])
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
    }

    // List all splits on an index
    {
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_1.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_2.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_3.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_4.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let result = metastore
            .stage_split(index_id, split_metadata_5.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        let time_range_opt = Some(Range {
            start: 0i64,
            end: 99i64,
        });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, time_range_opt, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), true);
        assert_eq!(split_ids.contains("two"), false);
        assert_eq!(split_ids.contains("three"), false);
        assert_eq!(split_ids.contains("four"), false);
        assert_eq!(split_ids.contains("five"), true);

        let time_range_opt = Some(Range {
            start: 200,
            end: i64::MAX,
        });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, time_range_opt, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), false);
        assert_eq!(split_ids.contains("two"), false);
        assert_eq!(split_ids.contains("three"), true);
        assert_eq!(split_ids.contains("four"), true);
        assert_eq!(split_ids.contains("five"), true);

        let time_range_opt = Some(Range {
            start: i64::MIN,
            end: 200,
        });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, time_range_opt, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), true);
        assert_eq!(split_ids.contains("two"), true);
        assert_eq!(split_ids.contains("three"), false);
        assert_eq!(split_ids.contains("four"), false);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range { start: 0, end: 100 });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), true);
        assert_eq!(split_ids.contains("two"), false);
        assert_eq!(split_ids.contains("three"), false);
        assert_eq!(split_ids.contains("four"), false);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range { start: 0, end: 101 });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), true);
        assert_eq!(split_ids.contains("two"), true);
        assert_eq!(split_ids.contains("three"), false);
        assert_eq!(split_ids.contains("four"), false);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range { start: 0, end: 199 });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), true);
        assert_eq!(split_ids.contains("two"), true);
        assert_eq!(split_ids.contains("three"), false);
        assert_eq!(split_ids.contains("four"), false);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range { start: 0, end: 200 });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), true);
        assert_eq!(split_ids.contains("two"), true);
        assert_eq!(split_ids.contains("three"), false);
        assert_eq!(split_ids.contains("four"), false);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range { start: 0, end: 201 });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), true);
        assert_eq!(split_ids.contains("two"), true);
        assert_eq!(split_ids.contains("three"), true);
        assert_eq!(split_ids.contains("four"), false);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range { start: 0, end: 299 });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), true);
        assert_eq!(split_ids.contains("two"), true);
        assert_eq!(split_ids.contains("three"), true);
        assert_eq!(split_ids.contains("four"), false);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range { start: 0, end: 300 });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), true);
        assert_eq!(split_ids.contains("two"), true);
        assert_eq!(split_ids.contains("three"), true);
        assert_eq!(split_ids.contains("four"), false);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range { start: 0, end: 301 });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), true);
        assert_eq!(split_ids.contains("two"), true);
        assert_eq!(split_ids.contains("three"), true);
        assert_eq!(split_ids.contains("four"), true);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range {
            start: 301,
            end: 400,
        });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), false);
        assert_eq!(split_ids.contains("two"), false);
        assert_eq!(split_ids.contains("three"), false);
        assert_eq!(split_ids.contains("four"), true);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range {
            start: 300,
            end: 400,
        });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), false);
        assert_eq!(split_ids.contains("two"), false);
        assert_eq!(split_ids.contains("three"), false);
        assert_eq!(split_ids.contains("four"), true);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range {
            start: 299,
            end: 400,
        });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), false);
        assert_eq!(split_ids.contains("two"), false);
        assert_eq!(split_ids.contains("three"), true);
        assert_eq!(split_ids.contains("four"), true);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range {
            start: 201,
            end: 400,
        });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), false);
        assert_eq!(split_ids.contains("two"), false);
        assert_eq!(split_ids.contains("three"), true);
        assert_eq!(split_ids.contains("four"), true);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range {
            start: 200,
            end: 400,
        });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), false);
        assert_eq!(split_ids.contains("two"), false);
        assert_eq!(split_ids.contains("three"), true);
        assert_eq!(split_ids.contains("four"), true);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range {
            start: 199,
            end: 400,
        });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), false);
        assert_eq!(split_ids.contains("two"), true);
        assert_eq!(split_ids.contains("three"), true);
        assert_eq!(split_ids.contains("four"), true);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range {
            start: 101,
            end: 400,
        });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), false);
        assert_eq!(split_ids.contains("two"), true);
        assert_eq!(split_ids.contains("three"), true);
        assert_eq!(split_ids.contains("four"), true);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range {
            start: 101,
            end: 400,
        });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), false);
        assert_eq!(split_ids.contains("two"), true);
        assert_eq!(split_ids.contains("three"), true);
        assert_eq!(split_ids.contains("four"), true);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range {
            start: 100,
            end: 400,
        });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), false);
        assert_eq!(split_ids.contains("two"), true);
        assert_eq!(split_ids.contains("three"), true);
        assert_eq!(split_ids.contains("four"), true);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range {
            start: 99,
            end: 400,
        });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), true);
        assert_eq!(split_ids.contains("two"), true);
        assert_eq!(split_ids.contains("three"), true);
        assert_eq!(split_ids.contains("four"), true);
        assert_eq!(split_ids.contains("five"), true);

        let range = Some(Range {
            start: 1000,
            end: 1100,
        });
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), false);
        assert_eq!(split_ids.contains("two"), false);
        assert_eq!(split_ids.contains("three"), false);
        assert_eq!(split_ids.contains("four"), false);
        assert_eq!(split_ids.contains("five"), true);

        let range = None;
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &[])
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|metadata| metadata.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains("one"), true);
        assert_eq!(split_ids.contains("two"), true);
        assert_eq!(split_ids.contains("three"), true);
        assert_eq!(split_ids.contains("four"), true);
        assert_eq!(split_ids.contains("five"), true);

        let range = None;
        let tags = vec!["bar".to_string(), "baz".to_string()];
        let splits = metastore
            .list_splits(index_id, SplitState::Staged, range, &tags)
            .await
            .unwrap();
        let split_ids: HashSet<String> = splits
            .into_iter()
            .map(|meta| meta.split_metadata.split_id)
            .collect();
        assert_eq!(split_ids.contains(&"one".to_string()), true);
        assert_eq!(split_ids.contains(&"two".to_string()), true);
        assert_eq!(split_ids.contains(&"three".to_string()), true);
        assert_eq!(split_ids.contains(&"four".to_string()), false);
        assert_eq!(split_ids.contains(&"five".to_string()), true);

        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }
}

#[allow(dead_code)]
pub async fn test_metastore_split_update_timestamp<MetastoreToTest: Metastore + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let mut current_timestamp = Utc::now().timestamp();

    let index_id = "my-index";
    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: "ram://indexes/my-index".to_string(),
        index_config: Arc::new(AllFlattenIndexConfig::default()),
        checkpoint: Checkpoint::default(),
    };

    let split_id = "one";
    let split_metadata = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: split_id.to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            generation: 3,
            update_timestamp: current_timestamp,
            ..Default::default()
        },
    };

    // Create an index
    metastore
        .create_index(index_metadata.clone())
        .await
        .unwrap();

    // wait for 1s, stage split & check `update_timestamp`
    sleep(Duration::from_secs(1)).await;
    metastore
        .stage_split(index_id, split_metadata.clone())
        .await
        .unwrap();
    let split_meta = metastore.list_all_splits(index_id).await.unwrap()[0]
        .clone()
        .split_metadata;
    assert!(split_meta.update_timestamp > current_timestamp);
    current_timestamp = split_meta.update_timestamp;

    // wait for 1s, publish split & check `update_timestamp`
    sleep(Duration::from_secs(1)).await;
    metastore
        .publish_splits(index_id, &[split_id], CheckpointDelta::from(0..5))
        .await
        .unwrap();
    let split_meta = metastore.list_all_splits(index_id).await.unwrap()[0]
        .clone()
        .split_metadata;
    assert!(split_meta.update_timestamp > current_timestamp);
    current_timestamp = split_meta.update_timestamp;

    // wait for 1s, mark split as deleted & check `update_timestamp`
    sleep(Duration::from_secs(1)).await;
    metastore
        .mark_splits_as_deleted(index_id, &[split_id])
        .await
        .unwrap();
    let split_meta = metastore.list_all_splits(index_id).await.unwrap()[0]
        .clone()
        .split_metadata;
    assert!(split_meta.update_timestamp > current_timestamp);
}

#[allow(dead_code)]
pub async fn test_metastore_storage_failing<MetastoreToTest: Metastore + DefaultForTest>() {
    let metastore = MetastoreToTest::default_for_test().await;

    let current_timestamp = Utc::now().timestamp();

    let index_id = "my-index";
    let index_metadata = IndexMetadata {
        index_id: index_id.to_string(),
        index_uri: "ram://my-indexes/my-index".to_string(),
        index_config: Arc::new(AllFlattenIndexConfig::default()),
        checkpoint: Checkpoint::default(),
    };

    let split_id = "one";
    let split_metadata = SplitMetadataAndFooterOffsets {
        footer_offsets: 1000..2000,
        split_metadata: SplitMetadata {
            split_id: split_id.to_string(),
            split_state: SplitState::Staged,
            num_records: 1,
            size_in_bytes: 2,
            time_range: None,
            generation: 3,
            update_timestamp: current_timestamp,
            ..Default::default()
        },
    };

    // create index
    metastore.create_index(index_metadata).await.unwrap();

    // stage split
    metastore
        .stage_split(index_id, split_metadata)
        .await
        .unwrap();

    // publish split fails
    let result = metastore
        .publish_splits(index_id, &[split_id], CheckpointDelta::default())
        .await
        .unwrap_err();
    assert!(matches!(result, MetastoreError::InternalError { .. }));

    // empty
    let split = metastore
        .list_splits(index_id, SplitState::Published, None, &[])
        .await
        .unwrap();
    assert!(split.is_empty());

    // not empty
    let split = metastore
        .list_splits(index_id, SplitState::Staged, None, &[])
        .await
        .unwrap();
    assert!(!split.is_empty());
}

macro_rules! metastore_test_suite {
    ($metastore_type:ty) => {
        #[cfg(test)]
        mod common_tests {
            #[tokio::test]
            async fn test_metastore_create_index() {
                crate::tests::test_metastore_create_index::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_delete_index() {
                crate::tests::test_metastore_delete_index::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_index_metadata() {
                crate::tests::test_metastore_index_metadata::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_stage_split() {
                crate::tests::test_metastore_stage_split::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_publish_splits() {
                crate::tests::test_metastore_publish_splits::<$metastore_type>().await;
            }

            // #[tokio::test]
            // async fn test_metastore_replace_splits() {
            //     crate::tests::test_metastore_replace_splits::<$metastore_type>().await;
            // }

            #[tokio::test]
            async fn test_metastore_mark_splits_as_deleted() {
                crate::tests::test_metastore_mark_splits_as_deleted::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_delete_splits() {
                crate::tests::test_metastore_delete_splits::<$metastore_type>().await;
            }

            // #[tokio::test]
            // async fn test_metastore_list_all_splits() {
            //     crate::tests::test_metastore_list_all_splits::<$metastore_type>().await;
            // }

            // #[tokio::test]
            // async fn test_metastore_list_splits() {
            //     crate::tests::test_metastore_list_splits::<$metastore_type>().await;
            // }

            // #[tokio::test]
            // async fn test_metastore_split_update_timestamp() {
            //     crate::tests::test_metastore_split_update_timestamp::<$metastore_type>().await;
            // }

            // #[tokio::test]
            // async fn test_metastore_storage_failing() {
            //     crate::tests::test_metastore_split_update_timestamp::<$metastore_type>().await;
            // }
        }
    };
}
