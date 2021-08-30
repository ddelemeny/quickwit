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
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::result::DatabaseErrorKind;
use diesel::result::Error::DatabaseError;
use diesel::BoolExpressionMethods;
use diesel::Connection;
use diesel::ExpressionMethods;
use diesel::PgConnection;
use diesel::QueryDsl;
use diesel::RunQueryDsl;
use dotenv::dotenv;
use tracing::{debug, error, info};

use crate::metastore::CheckpointDelta;
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

/// PostgreSQL metastore implementation.
pub struct PostgresqlMetastore {
    connection_pool: Arc<Pool<ConnectionManager<PgConnection>>>,
}

impl PostgresqlMetastore {
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

        initialize_db(&*connection_pool).map_err(|err| MetastoreError::InternalError {
            message: "Failed to initialize database".to_string(),
            cause: anyhow::anyhow!(err),
        })?;

        Ok(PostgresqlMetastore { connection_pool })
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

        let index_model = model::Index {
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
            diesel::insert_into(schema::indexes::dsl::indexes)
                .values(&index_model)
                .execute(&*conn)?;

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
            let num =
                diesel::delete(schema::indexes::dsl::indexes.find(index_id)).execute(&*conn)?;
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
        let conn = self
            .connection_pool
            .get()
            .map_err(|err| MetastoreError::InternalError {
                message: "Failed to get connection".to_string(),
                cause: anyhow::anyhow!(err),
            })?;

        // Check for the existence of split.
        let split_exists: bool = diesel::select(diesel::dsl::exists(
            schema::splits::dsl::splits.filter(
                schema::splits::dsl::index_id
                    .eq(index_id)
                    .and(schema::splits::dsl::split_id.eq(&metadata.split_metadata.split_id)),
            ),
        ))
        .get_result(&conn)
        .map_err(|err| MetastoreError::InternalError {
            message: "Failed to check for the existence of a split".to_string(),
            cause: anyhow::anyhow!(err),
        })?;
        if split_exists {
            return Err(MetastoreError::InternalError {
                message: format!(
                    "Try to stage split that already exists ({})",
                    metadata.split_metadata.split_id
                ),
                cause: anyhow::anyhow!(""),
            });
        }

        // Serialize the tags to fit the database model.
        let tags = serde_json::to_string(&metadata.split_metadata.tags).map_err(|err| {
            MetastoreError::InternalError {
                message: "Failed to serialize tags".to_string(),
                cause: anyhow::anyhow!(err),
            }
        })?;

        // Fit the time_range to the database model.
        let start_time_range = if let Some(range) = metadata.split_metadata.time_range.clone() {
            Some(range.start().clone())
        } else {
            None
        };
        let end_time_range = if let Some(range) = metadata.split_metadata.time_range.clone() {
            Some(range.end().clone())
        } else {
            None
        };

        // Insert a new split metadata as `Staged` state.
        let split_model = model::Split {
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
            diesel::insert_into(schema::splits::dsl::splits)
                .values(&split_model)
                .execute(&*conn)?;

            Ok(())
        })
        .map_err(|err| MetastoreError::InternalError {
            message: "Failed to stage split".to_string(),
            cause: anyhow::anyhow!(err),
        })?;

        Ok(())
    }

    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
        checkpoint_delta: CheckpointDelta,
    ) -> MetastoreResult<()> {
        unimplemented!()
    }

    async fn list_splits(
        &self,
        index_id: &str,
        state: SplitState,
        time_range_opt: Option<Range<i64>>,
        tags: &[String],
    ) -> MetastoreResult<Vec<SplitMetadataAndFooterOffsets>> {
        unimplemented!()
    }

    async fn list_all_splits(
        &self,
        index_id: &str,
    ) -> MetastoreResult<Vec<SplitMetadataAndFooterOffsets>> {
        unimplemented!()
    }

    async fn mark_splits_as_deleted<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        unimplemented!()
    }

    async fn delete_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        unimplemented!()
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        unimplemented!()
    }

    fn uri(&self) -> String {
        unimplemented!()
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
        let metastore = PostgresqlMetastore::new(uri)
            .await
            .map_err(|err| MetastoreResolverError::FailedToOpenMetastore(err))?;

        Ok(Arc::new(metastore))
    }
}
