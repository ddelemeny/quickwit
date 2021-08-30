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

use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use num::FromPrimitive;

use quickwit_index_config::{DefaultIndexConfigBuilder, IndexConfig};

use crate::checkpoint::Checkpoint;
use crate::schema::{indexes, splits};
use crate::IndexMetadata;
use crate::SplitMetadata;
use crate::SplitMetadataAndFooterOffsets;
use crate::SplitState;

/// A model structure for handling index metadata in a database.
#[derive(Identifiable, Insertable, Queryable, Debug)]
#[primary_key(index_id)]
#[table_name = "indexes"]
pub struct Index {
    /// Index ID. The index ID identifies the index when querying the metastore.
    pub index_id: String,
    /// Index URI. The index URI defines the location of the storage that contains the split.
    pub index_uri: String,
    /// The config used for this index. It is serialized to a string in json format.
    pub index_config: String,
    /// Checkpoint relative to a source. It express up to where documents have been indexed.
    /// It is serialized to a string in json format.
    pub checkpoint: String,
}

impl Index {
    pub fn make_index_config(&self) -> anyhow::Result<Arc<dyn IndexConfig>> {
        let builder: DefaultIndexConfigBuilder =
            serde_json::from_str(self.index_config.as_str()).map_err(|err| anyhow::anyhow!(err))?;

        let index_config = Arc::new(builder.build()?) as Arc<dyn IndexConfig>;

        Ok(index_config)
    }

    pub fn make_checkpoint(&self) -> anyhow::Result<Checkpoint> {
        serde_json::from_str::<Checkpoint>(self.checkpoint.as_str())
            .map_err(|err| anyhow::anyhow!(err))
    }

    pub fn make_index_metadata(&self) -> anyhow::Result<IndexMetadata> {
        let index_config = self.make_index_config()?;

        let checkpoint = self.make_checkpoint()?;

        Ok(IndexMetadata {
            index_id: self.index_id.clone(),
            index_uri: self.index_uri.clone(),
            index_config,
            checkpoint,
        })
    }
}

/// A model structure for handling split metadata in a database.
#[derive(Identifiable, Insertable, Associations, Queryable, Debug)]
#[belongs_to(Index)]
#[primary_key(split_id)]
#[table_name = "splits"]
pub struct Split {
    /// Split ID.
    pub split_id: String,
    /// The state of the split. This is the only mutable attribute of the split.
    pub split_state: i32,
    /// Number of records (or documents) in the split.
    pub num_records: i64,
    /// Sum of the size (in bytes) of the documents in this split.
    pub size_in_bytes: i64,
    /// If a timestamp field is available, the min timestamp in the split.
    pub start_time_range: Option<i64>,
    /// If a timestamp field is available, the max timestamp in the split.
    pub end_time_range: Option<i64>,
    /// Number of merges this segment has been subjected to during its lifetime.
    pub generation: i64,
    /// Timestamp for tracking when the split state was last modified.
    pub update_timestamp: i64,
    /// A list of tags for categorizing and searching group of splits.
    pub tags: String,
    /// Contains the start position of range of bytes of the footer that needs to be downloaded
    /// in order to open a split.
    pub start_footer_offset: i64,
    /// Contains the end position of range of bytes of the footer that needs to be downloaded
    /// in order to open a split.
    pub end_footer_offset: i64,
    /// Index ID. It is used as a foreign key in the database.
    pub index_id: String,
}

impl Split {
    /// Make time range from start_time_range and end_time_range in database model.
    pub fn get_time_range(&self) -> Option<RangeInclusive<i64>> {
        self.start_time_range.and_then(|start_time_range| {
            self.end_time_range
                .map(|end_time_range| RangeInclusive::new(start_time_range, end_time_range))
        })
    }

    /// Get split state from split_state in database model.
    pub fn get_split_state(&self) -> Option<SplitState> {
        SplitState::from_i64(self.split_state as i64)
    }

    /// Get tags from serialized tags.
    pub fn get_tags(&self) -> anyhow::Result<Vec<String>> {
        serde_json::from_str(self.tags.as_str()).map_err(|err| anyhow::anyhow!(err))
    }

    pub fn make_split_metadata(&self) -> anyhow::Result<SplitMetadata> {
        let time_range = self.get_time_range();

        let split_state = self
            .get_split_state()
            .ok_or_else(|| anyhow::anyhow!("Unknown split state {:?}", self.split_state))?;

        let tags = self.get_tags()?;

        Ok(SplitMetadata {
            split_id: self.split_id.clone(),
            num_records: self.num_records as usize,
            size_in_bytes: self.size_in_bytes as u64,
            time_range,
            generation: self.generation as usize,
            split_state,
            update_timestamp: self.update_timestamp,
            tags,
        })
    }

    pub fn make_split_metadata_and_footer_offsets(
        &self,
    ) -> anyhow::Result<SplitMetadataAndFooterOffsets> {
        let split_metadata = self.make_split_metadata()?;
        let footer_offsets = Range {
            start: self.start_footer_offset as u64,
            end: self.end_footer_offset as u64,
        };

        Ok(SplitMetadataAndFooterOffsets {
            split_metadata,
            footer_offsets,
        })
    }
}
