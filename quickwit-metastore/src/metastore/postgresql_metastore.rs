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

use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use diesel::debug_query;
use diesel::pg::Pg;
use diesel::r2d2::{ConnectionManager, Pool, PooledConnection};
use diesel::result::DatabaseErrorKind;
use diesel::result::Error::DatabaseError;
use diesel::QueryResult;
use diesel::{
    BoolExpressionMethods, Connection, ExpressionMethods, PgConnection, QueryDsl, RunQueryDsl,
};
use dotenv::dotenv;
use tokio::sync::OnceCell;
use tracing::{debug, error, info};

use crate::metastore::{Checkpoint, CheckpointDelta};
use crate::model;
use crate::schema;
use crate::IndexMetadata;
use crate::Metastore;
use crate::MetastoreError;
use crate::MetastoreFactory;
use crate::MetastoreResolverError;
use crate::MetastoreResult;
use crate::SplitMetadataAndFooterOffsets;
use crate::SplitState;

embed_migrations!();

const MAX_CONNECTION_POOL_SIZE: u32 = 10;
const CONNECTION_TIMEOUT: u64 = 10;
const MAX_CONNECTION_RETRY_COUNT: u32 = 10;
const CONNECTION_STATUS_CHECK_INTERVAL: u64 = 2;

/// Establishes a connection to the given database URI.
fn establish_connection(
    database_uri: &str,
) -> anyhow::Result<Pool<ConnectionManager<PgConnection>>> {
    dotenv().ok();

    let mut retry_cnt = 0;

    loop {
        let connection_manager: ConnectionManager<PgConnection> =
            ConnectionManager::new(database_uri);
        let pool_res = Pool::builder()
            .max_size(MAX_CONNECTION_POOL_SIZE)
            .connection_timeout(Duration::from_secs(CONNECTION_TIMEOUT))
            .build(connection_manager);
        match pool_res {
            Ok(pool) => {
                return Ok(pool);
            }
            Err(err) => {
                error!(err=?err, "Failed to connect to postgres. Trying again");

                if retry_cnt > MAX_CONNECTION_RETRY_COUNT {
                    anyhow::bail!(
                        "The retry count has exceeded the limit ({})",
                        MAX_CONNECTION_RETRY_COUNT
                    );
                }

                retry_cnt += 1;
                continue;
            }
        }
    }
}

/// Initialize the database.
/// The sql used for the initialization is stored in quickwit-metastore/migrations directory.
fn initialize_db(pool: &Pool<ConnectionManager<PgConnection>>) -> anyhow::Result<()> {
    let db_conn = pool.get()?;
    let mut migrations_log_buffer = Vec::new();
    let embedded_migrations_res =
        embedded_migrations::run_with_output(&*db_conn, &mut migrations_log_buffer);

    match &embedded_migrations_res {
        Ok(()) => {
            let migrations_log = String::from_utf8_lossy(&migrations_log_buffer);
            info!(
                migration_output = migrations_log.as_ref(),
                "Database migrations succeeded"
            );
        }
        Err(ref migration_err) => {
            let migrations_log = String::from_utf8_lossy(&migrations_log_buffer);
            error!(err=%migration_err, migration_output=migrations_log.as_ref(), "Database migrations failed");
        }
    }

    embedded_migrations_res?;

    Ok(())
}

/// Get the PostgreSQL-based metastore. The metastore is created as a singleton.
pub async fn get_postgresql_metastore(database_uri: &str) -> &'static PostgresqlMetastore {
    static POSTGRESQL_METASTORE: OnceCell<PostgresqlMetastore> = OnceCell::const_new();
    POSTGRESQL_METASTORE
        .get_or_init(|| async {
            PostgresqlMetastore::new(database_uri)
                .await
                .expect("PostgreSQL metastore is not initialized")
        })
        .await
}

/// PostgreSQL metastore implementation.
#[derive(Clone)]
pub struct PostgresqlMetastore {
    uri: String,
    connection_pool: Arc<Pool<ConnectionManager<PgConnection>>>,
}

impl PostgresqlMetastore {
    /// Creates a meta store given a storage.
    pub async fn new(database_uri: &str) -> MetastoreResult<Self> {
        let connection_pool = Arc::new(establish_connection(database_uri).map_err(|err| {
            MetastoreError::ConnectionError {
                message: err.to_string(),
            }
        })?);

        let connection_pool_clone = connection_pool.clone();
        loop {
            let connection_pool_status = connection_pool_clone.state();
            debug!(
                connections = connection_pool_status.connections,
                idle_connections = connection_pool_status.idle_connections,
                "Connection pool status"
            );
            let conn_res = connection_pool_clone.get();
            if conn_res.is_ok() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(
                CONNECTION_STATUS_CHECK_INTERVAL,
            ))
            .await;
        }

        match initialize_db(&*connection_pool) {
            Ok(_) => info!("Database initialized"),
            Err(err) => error!("Failed to initialize database {:?}", err),
        }

        Ok(PostgresqlMetastore {
            uri: database_uri.to_string(),
            connection_pool,
        })
    }

    /// Check index existence.
    /// Returns true if the index exists.
    fn is_index_exist(
        &self,
        conn: &PooledConnection<ConnectionManager<PgConnection>>,
        index_id: &str,
    ) -> QueryResult<bool> {
        let check_index_existence_statement = diesel::select(diesel::dsl::exists(
            schema::indexes::dsl::indexes.filter(schema::indexes::dsl::index_id.eq(index_id)),
        ));
        debug!(sql=%debug_query::<Pg, _>(&check_index_existence_statement).to_string());
        let index_exists: bool = check_index_existence_statement.get_result(conn)?;

        Ok(index_exists)
    }

    ///
    fn get_non_publishable_splits(
        &self,
        conn: &PooledConnection<ConnectionManager<PgConnection>>,
        index_id: &str,
        split_ids: &[&str],
    ) -> QueryResult<Vec<String>> {
        let select_non_publishable_splits_statement = schema::splits::dsl::splits.filter(
            schema::splits::dsl::index_id.eq(index_id).and(
                schema::splits::dsl::split_id.eq_any(split_ids).and(
                    schema::splits::dsl::split_state
                        .ne(SplitState::Staged as i32)
                        .and(schema::splits::dsl::split_state.ne(SplitState::Published as i32)),
                ),
            ),
        );
        debug!(sql=%debug_query::<Pg, _>(&select_non_publishable_splits_statement).to_string());
        let non_publishable_splits: Vec<model::Split> =
            select_non_publishable_splits_statement.get_results(conn)?;

        let non_publishable_split_ids = non_publishable_splits
            .iter()
            .map(|split| split.split_id.clone())
            .collect();

        Ok(non_publishable_split_ids)
    }

    /// Publish splits.
    /// Returns the successful split IDs.
    fn publish_splits(
        &self,
        conn: &PooledConnection<ConnectionManager<PgConnection>>,
        index_id: &str,
        split_ids: &[&str],
    ) -> QueryResult<Vec<String>> {
        let update_splits_statement = diesel::update(
            schema::splits::dsl::splits.filter(
                schema::splits::dsl::index_id
                    .eq(index_id)
                    .and(schema::splits::dsl::split_id.eq_any(split_ids)),
            ),
        )
        .set((
            schema::splits::dsl::split_state.eq(SplitState::Published as i32),
            schema::splits::dsl::update_timestamp.eq(Utc::now().timestamp()),
        ));
        debug!(sql=%debug_query::<Pg, _>(&update_splits_statement).to_string());
        let updated_splits: Vec<model::Split> = update_splits_statement.get_results(&*conn)?;

        let succeeded_split_ids: Vec<String> = updated_splits
            .iter()
            .map(|split| split.split_id.clone())
            .collect();

        Ok(succeeded_split_ids)
    }

    /// Mark splits as deleted.
    /// Returns the successful split IDs.
    fn mark_as_deleted(
        &self,
        conn: &PooledConnection<ConnectionManager<PgConnection>>,
        index_id: &str,
        split_ids: &[&str],
    ) -> QueryResult<Vec<String>> {
        let update_splits_statement = diesel::update(
            schema::splits::dsl::splits.filter(
                schema::splits::dsl::index_id
                    .eq(index_id)
                    .and(schema::splits::dsl::split_id.eq_any(split_ids)),
            ),
        )
        .set((
            schema::splits::dsl::split_state.eq(SplitState::ScheduledForDeletion as i32),
            schema::splits::dsl::update_timestamp.eq(Utc::now().timestamp()),
        ));
        debug!(sql=%debug_query::<Pg, _>(&update_splits_statement).to_string());
        let updated_splits: Vec<model::Split> = update_splits_statement.get_results(&*conn)?;

        let succeeded_split_ids: Vec<String> = updated_splits
            .iter()
            .map(|split| split.split_id.clone())
            .collect();

        Ok(succeeded_split_ids)
    }
}

#[async_trait]
impl Metastore for PostgresqlMetastore {
    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()> {
        // Serialize the index_config to fit the database model.
        let index_config_str =
            serde_json::to_string(&index_metadata.index_config).map_err(|err| {
                MetastoreError::InternalError {
                    message: "Failed to serialize index config".to_string(),
                    cause: anyhow::anyhow!(err),
                }
            })?;

        // Serialize the checkpoint to fit the database model.
        let checkpoint_str = serde_json::to_string(&index_metadata.checkpoint).map_err(|err| {
            MetastoreError::InternalError {
                message: "Failed to serialize checkpoint".to_string(),
                cause: anyhow::anyhow!(err),
            }
        })?;

        let model_index = model::Index {
            index_id: index_metadata.index_id.clone(),
            index_uri: index_metadata.index_uri.clone(),
            index_config: index_config_str,
            checkpoint: checkpoint_str,
        };

        let conn = self
            .connection_pool
            .get()
            .map_err(|err| MetastoreError::InternalError {
                message: "Failed to get connection".to_string(),
                cause: anyhow::anyhow!(err),
            })?;

        conn.transaction::<_, diesel::result::Error, _>(|| {
            // Create index.
            let create_index_statement =
                diesel::insert_into(schema::indexes::dsl::indexes).values(&model_index);
            debug!(sql=%debug_query::<Pg, _>(&create_index_statement).to_string());
            create_index_statement.execute(&*conn)?;

            Ok(())
        })
        .map_err(|err| match err {
            DatabaseError(kind, err_info) => match kind {
                DatabaseErrorKind::UniqueViolation => MetastoreError::IndexAlreadyExists {
                    index_id: index_metadata.index_id.clone(),
                },
                _ => MetastoreError::InternalError {
                    message: "Failed to create index".to_string(),
                    cause: anyhow::anyhow!(err_info.message().to_string()),
                },
            },
            _ => MetastoreError::InternalError {
                message: "Failed to create index".to_string(),
                cause: anyhow::anyhow!(err),
            },
        })?;

        Ok(())
    }

    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        let conn = self
            .connection_pool
            .get()
            .map_err(|err| MetastoreError::InternalError {
                message: "Failed to get connection".to_string(),
                cause: anyhow::anyhow!(err),
            })?;

        conn.transaction::<_, diesel::result::Error, _>(|| {
            // Delete index.
            let delete_index_statement =
                diesel::delete(schema::indexes::dsl::indexes.find(index_id));
            debug!(sql=%debug_query::<Pg, _>(&delete_index_statement).to_string());
            let num = delete_index_statement.execute(&*conn)?;
            if num == 0 {
                return Err(diesel::result::Error::NotFound);
            }

            Ok(())
        })
        .map_err(|err| match err {
            diesel::result::Error::NotFound => MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            },
            _ => MetastoreError::InternalError {
                message: "Failed to delete index".to_string(),
                cause: anyhow::anyhow!(err),
            },
        })?;

        Ok(())
    }

    async fn stage_split(
        &self,
        index_id: &str,
        metadata: SplitMetadataAndFooterOffsets,
    ) -> MetastoreResult<()> {
        // Serialize the tags to fit the database model.
        let tags = serde_json::to_string(&metadata.split_metadata.tags).map_err(|err| {
            MetastoreError::InternalError {
                message: "Failed to serialize tags".to_string(),
                cause: anyhow::anyhow!(err),
            }
        })?;

        // Fit the time_range to the database model.
        let start_time_range = metadata
            .split_metadata
            .time_range
            .clone()
            .map(|range| *range.start());
        let end_time_range = metadata
            .split_metadata
            .time_range
            .clone()
            .map(|range| *range.end());

        let conn = self
            .connection_pool
            .get()
            .map_err(|err| MetastoreError::InternalError {
                message: "Failed to get connection".to_string(),
                cause: anyhow::anyhow!(err),
            })?;

        let model_split = model::Split {
            split_id: metadata.split_metadata.split_id,
            split_state: SplitState::Staged as i32,
            num_records: metadata.split_metadata.num_records as i64,
            size_in_bytes: metadata.split_metadata.size_in_bytes as i64,
            start_time_range,
            end_time_range,
            generation: metadata.split_metadata.generation as i64,
            update_timestamp: Utc::now().timestamp(),
            tags,
            start_footer_offset: metadata.footer_offsets.start as i64,
            end_footer_offset: metadata.footer_offsets.end as i64,
            index_id: index_id.to_string(),
        };

        conn.transaction::<_, diesel::result::Error, _>(|| {
            // Insert a new split metadata as `Staged` state.
            let insert_staged_split_statement =
                diesel::insert_into(schema::splits::dsl::splits).values(&model_split);
            debug!(sql=%debug_query::<Pg, _>(&insert_staged_split_statement).to_string());
            insert_staged_split_statement.execute(&*conn)?;

            Ok(())
        })
        .map_err(|err| match err {
            DatabaseError(kind, err_info) => match kind {
                DatabaseErrorKind::ForeignKeyViolation => MetastoreError::IndexDoesNotExist {
                    index_id: index_id.to_string(),
                },
                _ => MetastoreError::InternalError {
                    message: "Failed to create index".to_string(),
                    cause: anyhow::anyhow!(err_info.message().to_string()),
                },
            },
            _ => MetastoreError::InternalError {
                message: "Failed to create index".to_string(),
                cause: anyhow::anyhow!(err),
            },
        })?;

        Ok(())
    }

    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        checkpoint_delta: CheckpointDelta,
    ) -> MetastoreResult<()> {
        let conn = self
            .connection_pool
            .get()
            .map_err(|err| MetastoreError::InternalError {
                message: "Failed to get connection".to_string(),
                cause: anyhow::anyhow!(err),
            })?;

        // Get index metadata.
        let select_index_statement =
            schema::indexes::dsl::indexes.filter(schema::indexes::dsl::index_id.eq(index_id));
        debug!(sql=%debug_query::<Pg, _>(&select_index_statement).to_string());
        let model_index = select_index_statement
            .first::<model::Index>(&conn)
            .map_err(|err| match err {
                diesel::result::Error::NotFound => MetastoreError::IndexDoesNotExist {
                    index_id: index_id.to_string(),
                },
                _ => MetastoreError::InternalError {
                    message: "Failed to select index".to_string(),
                    cause: anyhow::anyhow!(err),
                },
            })?;

        // Deserialize the checkpoint from the database model.
        let mut checkpoint: Checkpoint =
            serde_json::from_str(&model_index.checkpoint).map_err(|err| {
                MetastoreError::InternalError {
                    message: "Failed to deserialize checkpoint".to_string(),
                    cause: anyhow::anyhow!(err),
                }
            })?;

        // Apply checkpoint_delta
        checkpoint.try_apply_delta(checkpoint_delta)?;

        // Serialize the checkpoint to fit the database model.
        let new_checkpoint =
            serde_json::to_string(&checkpoint).map_err(|err| MetastoreError::InternalError {
                message: "Failed to serialize checkpoint".to_string(),
                cause: anyhow::anyhow!(err),
            })?;

        // Check for the inclusion of non-publishable split IDs.
        // Except for SplitState::Staged and SplitState::Published, you cannot publish.
        let non_publishable_split_ids = self
            .get_non_publishable_splits(&conn, index_id, split_ids)
            .map_err(|err| MetastoreError::InternalError {
                message: "Failed to publish splits".to_string(),
                cause: anyhow::anyhow!(err),
            })?;
        if !non_publishable_split_ids.is_empty() {
            if let Some(non_publishable_split_id) = non_publishable_split_ids.first() {
                return Err(MetastoreError::SplitIsNotStaged {
                    split_id: non_publishable_split_id.clone(),
                });
            }
        }

        let mut error_split_id = "";
        conn.transaction::<_, diesel::result::Error, _>(|| {
            // Update the index checkpoint.
            let update_index_statement =
                diesel::update(schema::indexes::dsl::indexes.find(index_id))
                    .set(schema::indexes::dsl::checkpoint.eq(new_checkpoint));
            debug!(sql=%debug_query::<Pg, _>(&update_index_statement).to_string());
            update_index_statement.execute(&*conn)?;

            // Publish splits.
            let published_split_ids = self.publish_splits(&conn, index_id, split_ids)?;

            // Find out the ID of the update error and return an error if there is one.
            if published_split_ids.len() < split_ids.len() {
                for split_id in split_ids.iter() {
                    if !published_split_ids
                        .iter()
                        .any(|published_split_id| published_split_id == *split_id)
                    {
                        error_split_id = split_id;
                        break;
                    }
                }
                return Err(diesel::result::Error::NotFound);
            }

            Ok(())
        })
        .map_err(|err| match err {
            diesel::result::Error::NotFound => MetastoreError::SplitDoesNotExist {
                split_id: error_split_id.to_string(),
            },
            _ => MetastoreError::InternalError {
                message: "Failed to publish splits".to_string(),
                cause: anyhow::anyhow!(err),
            },
        })?;

        Ok(())
    }

    async fn replace_splits<'a>(
        &self,
        index_id: &str,
        new_split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let conn = self
            .connection_pool
            .get()
            .map_err(|err| MetastoreError::InternalError {
                message: "Failed to get connection".to_string(),
                cause: anyhow::anyhow!(err),
            })?;

        // Check for the existence of index.
        let index_exists: bool =
            self.is_index_exist(&conn, index_id)
                .map_err(|err| MetastoreError::InternalError {
                    message: "Failed to publish splits".to_string(),
                    cause: anyhow::anyhow!(err),
                })?;
        if !index_exists {
            return Err(MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        // Check for the inclusion of non-publishable split IDs.
        // Except for SplitState::Staged and SplitState::Published, you cannot publish.
        let non_publishable_split_ids = self
            .get_non_publishable_splits(&conn, index_id, new_split_ids)
            .map_err(|err| MetastoreError::InternalError {
                message: "Failed to publish splits".to_string(),
                cause: anyhow::anyhow!(err),
            })?;
        if !non_publishable_split_ids.is_empty() {
            if let Some(non_publishable_split_id) = non_publishable_split_ids.first() {
                return Err(MetastoreError::SplitIsNotStaged {
                    split_id: non_publishable_split_id.clone(),
                });
            }
        }

        let mut error_split_id = "";
        conn.transaction::<_, diesel::result::Error, _>(|| {
            // Publish splits.
            let published_split_ids = self.publish_splits(&conn, index_id, new_split_ids)?;

            // Find out the ID of the update error and return an error if there is one.
            if published_split_ids.len() < new_split_ids.len() {
                for split_id in new_split_ids.iter() {
                    if !published_split_ids
                        .iter()
                        .any(|published_split_id| published_split_id == *split_id)
                    {
                        error_split_id = split_id;
                        break;
                    }
                }
                return Err(diesel::result::Error::NotFound);
            }

            // Mark as deleted.
            let mark_as_deleted_split_ids =
                self.mark_as_deleted(&conn, index_id, replaced_split_ids)?;

            // Find out the ID of the update error and return an error if there is one.
            if mark_as_deleted_split_ids.len() < replaced_split_ids.len() {
                for split_id in replaced_split_ids.iter() {
                    if !mark_as_deleted_split_ids
                        .iter()
                        .any(|mark_as_deleted_split_id| mark_as_deleted_split_id == *split_id)
                    {
                        error_split_id = split_id;
                        break;
                    }
                }
                return Err(diesel::result::Error::NotFound);
            }

            Ok(())
        })
        .map_err(|err| match err {
            diesel::result::Error::NotFound => MetastoreError::SplitDoesNotExist {
                split_id: error_split_id.to_string(),
            },
            _ => MetastoreError::InternalError {
                message: "Failed to publish splits".to_string(),
                cause: anyhow::anyhow!(err),
            },
        })?;

        Ok(())
    }

    async fn list_splits(
        &self,
        index_id: &str,
        state: SplitState,
        time_range_opt: Option<Range<i64>>,
        tags: &[String],
    ) -> MetastoreResult<Vec<SplitMetadataAndFooterOffsets>> {
        let conn = self
            .connection_pool
            .get()
            .map_err(|err| MetastoreError::InternalError {
                message: "Failed to get connection".to_string(),
                cause: anyhow::anyhow!(err),
            })?;

        // Check for the existence of index.
        let index_exists: bool =
            self.is_index_exist(&conn, index_id)
                .map_err(|err| MetastoreError::InternalError {
                    message: "Failed to publish splits".to_string(),
                    cause: anyhow::anyhow!(err),
                })?;
        if !index_exists {
            return Err(MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        let list_splits_statement = if let Some(time_range) = time_range_opt {
            schema::splits::dsl::splits
                .filter(
                    schema::splits::dsl::index_id
                        .eq(index_id)
                        .and(schema::splits::dsl::split_state.eq(state as i32))
                        .and(
                            schema::splits::dsl::end_time_range.is_null().or(
                                schema::splits::dsl::end_time_range
                                    .ge(time_range.start)
                                    .and(schema::splits::dsl::start_time_range.lt(time_range.end)),
                            ),
                        ),
                )
                .into_boxed()
        } else {
            schema::splits::dsl::splits
                .filter(
                    schema::splits::dsl::index_id
                        .eq(index_id)
                        .and(schema::splits::dsl::split_state.eq(state as i32)),
                )
                .into_boxed()
        };
        debug!(sql=%debug_query::<Pg, _>(&list_splits_statement).to_string());
        let model_splits: Vec<model::Split> =
            list_splits_statement
                .load(&conn)
                .map_err(|err| MetastoreError::InternalError {
                    message: "Failed to select splits".to_string(),
                    cause: anyhow::anyhow!(err),
                })?;

        // Make the split metadata and footer offsets from database model.
        let mut split_metadata_footer_offset_list: Vec<SplitMetadataAndFooterOffsets> = Vec::new();
        for model_split in model_splits {
            let split_metadata_and_footer_offsets =
                match model_split.make_split_metadata_and_footer_offsets() {
                    Ok(metadata) => metadata,
                    Err(err) => {
                        error!("Failed to make split metadata and footer offsets {:?}", err);
                        continue;
                    }
                };

            // The tags are empty (unspecified) or the tags are specified
            // and the split has a matching tag, it will be added to the list.
            let mut match_tag = false;
            if tags.is_empty() {
                match_tag = true;
            } else {
                for tag in tags {
                    if split_metadata_and_footer_offsets
                        .split_metadata
                        .tags
                        .contains(tag)
                    {
                        match_tag = true;
                        break;
                    }
                }
            }
            if match_tag {
                split_metadata_footer_offset_list.push(split_metadata_and_footer_offsets);
            }
        }

        Ok(split_metadata_footer_offset_list)
    }

    async fn list_all_splits(
        &self,
        index_id: &str,
    ) -> MetastoreResult<Vec<SplitMetadataAndFooterOffsets>> {
        let conn = self
            .connection_pool
            .get()
            .map_err(|err| MetastoreError::InternalError {
                message: "Failed to get connection".to_string(),
                cause: anyhow::anyhow!(err),
            })?;

        // Check for the existence of index.
        let index_exists: bool =
            self.is_index_exist(&conn, index_id)
                .map_err(|err| MetastoreError::InternalError {
                    message: "Failed to publish splits".to_string(),
                    cause: anyhow::anyhow!(err),
                })?;
        if !index_exists {
            return Err(MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        let list_all_splits_statement =
            schema::splits::dsl::splits.filter(schema::splits::dsl::index_id.eq(index_id));
        debug!(sql=%debug_query::<Pg, _>(&list_all_splits_statement).to_string());
        let model_splits: Vec<model::Split> =
            list_all_splits_statement
                .load(&conn)
                .map_err(|err| MetastoreError::InternalError {
                    message: "Failed to select splits".to_string(),
                    cause: anyhow::anyhow!(err),
                })?;

        // Make the split metadata and footer offsets from database model.
        let mut split_metadata_footer_offset_list: Vec<SplitMetadataAndFooterOffsets> = Vec::new();
        for model_split in model_splits {
            let split_metadata_and_footer_offsets =
                match model_split.make_split_metadata_and_footer_offsets() {
                    Ok(metadata) => metadata,
                    Err(err) => {
                        error!("Failed to make split metadata and footer offsets {:?}", err);
                        continue;
                    }
                };
            split_metadata_footer_offset_list.push(split_metadata_and_footer_offsets);
        }

        Ok(split_metadata_footer_offset_list)
    }

    async fn mark_splits_as_deleted<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let conn = self
            .connection_pool
            .get()
            .map_err(|err| MetastoreError::InternalError {
                message: "Failed to get connection".to_string(),
                cause: anyhow::anyhow!(err),
            })?;

        // Check for the existence of index.
        let index_exists: bool =
            self.is_index_exist(&conn, index_id)
                .map_err(|err| MetastoreError::InternalError {
                    message: "Failed to publish splits".to_string(),
                    cause: anyhow::anyhow!(err),
                })?;
        if !index_exists {
            return Err(MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        let mut error_split_id = "";
        conn.transaction::<_, diesel::result::Error, _>(|| {
            // Mark as deleted.
            let mark_as_deleted_split_ids = self.mark_as_deleted(&conn, index_id, split_ids)?;

            // Find out the ID of the update error and return an error if there is one.
            if mark_as_deleted_split_ids.len() < split_ids.len() {
                for split_id in split_ids.iter() {
                    if !mark_as_deleted_split_ids
                        .iter()
                        .any(|mark_as_deleted_split_id| mark_as_deleted_split_id == *split_id)
                    {
                        error_split_id = split_id;
                        break;
                    }
                }
                return Err(diesel::result::Error::NotFound);
            }

            Ok(())
        })
        .map_err(|err| match err {
            diesel::result::Error::NotFound => MetastoreError::SplitDoesNotExist {
                split_id: error_split_id.to_string(),
            },
            _ => MetastoreError::InternalError {
                message: "Failed to mark splits as deleted".to_string(),
                cause: anyhow::anyhow!(err),
            },
        })?;

        Ok(())
    }

    async fn delete_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        let conn = self
            .connection_pool
            .get()
            .map_err(|err| MetastoreError::InternalError {
                message: "Failed to get connection".to_string(),
                cause: anyhow::anyhow!(err),
            })?;

        // Check for the existence of index.
        let index_exists: bool =
            self.is_index_exist(&conn, index_id)
                .map_err(|err| MetastoreError::InternalError {
                    message: "Failed to publish splits".to_string(),
                    cause: anyhow::anyhow!(err),
                })?;
        if !index_exists {
            return Err(MetastoreError::IndexDoesNotExist {
                index_id: index_id.to_string(),
            });
        }

        // Checks if the given sjplit IDs contains a split ID that cannot be deleted.
        let select_non_deletable_splits_statement = schema::splits::dsl::splits.filter(
            schema::splits::dsl::index_id.eq(index_id).and(
                schema::splits::dsl::split_id.eq_any(split_ids).and(
                    schema::splits::dsl::split_state
                        .ne(SplitState::Staged as i32)
                        .and(
                            schema::splits::dsl::split_state
                                .ne(SplitState::ScheduledForDeletion as i32),
                        ),
                ),
            ),
        );
        debug!(sql=%debug_query::<Pg, _>(&select_non_deletable_splits_statement).to_string());
        let non_deletable_splits: Vec<model::Split> = select_non_deletable_splits_statement
            .get_results(&conn)
            .map_err(|err| MetastoreError::InternalError {
                message: "Failed to select index".to_string(),
                cause: anyhow::anyhow!(err),
            })?;
        if !non_deletable_splits.is_empty() {
            let mut non_deletable_split_id = "".to_string();
            if let Some(non_deletable_split) = non_deletable_splits.first() {
                non_deletable_split_id = non_deletable_split.split_id.clone();
            }
            let message: String = format!(
                "This split {:?} is not in a deletable state",
                non_deletable_split_id
            );
            return Err(MetastoreError::Forbidden { message });
        }

        let mut error_split_id = "";
        conn.transaction::<_, diesel::result::Error, _>(|| {
            let delete_splits_statement = diesel::delete(
                schema::splits::dsl::splits.filter(
                    schema::splits::dsl::index_id.eq(index_id).and(
                        schema::splits::dsl::split_id.eq_any(split_ids).and(
                            schema::splits::dsl::split_state
                                .eq(SplitState::ScheduledForDeletion as i32)
                                .or(schema::splits::dsl::split_state.eq(SplitState::Staged as i32)),
                        ),
                    ),
                ),
            );
            debug!(sql=%debug_query::<Pg, _>(&delete_splits_statement).to_string());
            let deleted_splits: Vec<model::Split> = delete_splits_statement.get_results(&*conn)?;

            // Splits that are not deleted are treated as errors because they are non-existent splits.
            if deleted_splits.len() < split_ids.len() {
                for split_id in split_ids.iter() {
                    if !deleted_splits
                        .iter()
                        .map(|deleted_split| deleted_split.split_id.clone())
                        .any(|x| x == *split_id)
                    {
                        error_split_id = split_id;
                        break;
                    }
                }
                return Err(diesel::result::Error::NotFound);
            }

            Ok(())
        })
        .map_err(|err| match err {
            diesel::result::Error::NotFound => MetastoreError::SplitDoesNotExist {
                split_id: error_split_id.to_string(),
            },
            _ => MetastoreError::InternalError {
                message: "Failed to mark splits as deleted".to_string(),
                cause: anyhow::anyhow!(err),
            },
        })?;

        Ok(())
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        let conn = self
            .connection_pool
            .get()
            .map_err(|err| MetastoreError::InternalError {
                message: "Failed to get connection".to_string(),
                cause: anyhow::anyhow!(err),
            })?;

        let select_index_statement =
            schema::indexes::dsl::indexes.filter(schema::indexes::dsl::index_id.eq(index_id));
        debug!(sql=%debug_query::<Pg, _>(&select_index_statement).to_string());
        let model_index = select_index_statement
            .first::<model::Index>(&conn)
            .map_err(|err| match err {
                diesel::result::Error::NotFound => MetastoreError::IndexDoesNotExist {
                    index_id: index_id.to_string(),
                },
                _ => MetastoreError::InternalError {
                    message: "Failed to select index".to_string(),
                    cause: anyhow::anyhow!(err),
                },
            })?;

        let index_metadata =
            model_index
                .make_index_metadata()
                .map_err(|err| MetastoreError::InternalError {
                    message: "Failed to make index metadata".to_string(),
                    cause: anyhow::anyhow!(err),
                })?;

        Ok(index_metadata)
    }

    fn uri(&self) -> String {
        self.uri.clone()
    }
}

/// A single file metastore factory
#[derive(Clone)]
pub struct PostgresqlMetastoreFactory {}

impl Default for PostgresqlMetastoreFactory {
    fn default() -> Self {
        PostgresqlMetastoreFactory {}
    }
}

#[async_trait]
impl MetastoreFactory for PostgresqlMetastoreFactory {
    async fn resolve(&self, uri: &str) -> Result<Arc<dyn Metastore>, MetastoreResolverError> {
        let metastore = get_postgresql_metastore(uri).await;

        Ok(Arc::new((*metastore).clone()))
    }
}

#[cfg(test)]
#[async_trait]
impl crate::tests::test_suite::DefaultForTest for PostgresqlMetastore {
    async fn default_for_test() -> Self {
        use std::env;

        dotenv().ok();

        let database_url = env::var("TEST_DATABASE_URL").unwrap();
        let metastore = get_postgresql_metastore(&database_url).await;

        (*metastore).clone()
    }
}

metastore_test_suite_for_database!(crate::PostgresqlMetastore);
