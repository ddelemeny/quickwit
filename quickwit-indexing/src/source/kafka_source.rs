use crate::models::RawDocBatch;
use crate::source::IndexerMessage;
use crate::source::Source;
use crate::source::SourceContext;
use crate::source::TypedSourceFactory;
use anyhow::{bail, Context};
use async_trait::async_trait;
use futures::StreamExt;
use quickwit_actors::ActorExitStatus;
use quickwit_actors::Mailbox;
use quickwit_metastore::checkpoint::{Checkpoint, CheckpointDelta, PartitionId, Position};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext, Rebalance};
use rdkafka::error::{KafkaError, KafkaResult};
use rdkafka::message::BorrowedMessage;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::Timeout;
use rdkafka::{ClientContext, Message, Offset};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::fmt;
use std::time::Duration;
use tracing::{debug, info, warn};

#[derive(Clone, Deserialize, Serialize)]
pub struct KafkaSourceParams {
    bootstrap_servers: String,
    group_id: String,
    topic: String,
    enable_partition_eof: Option<bool>,
}

impl KafkaSourceParams {
    pub fn new(
        bootstrap_servers: String,
        group_id: String,
        topic: String,
        enable_partition_eof: Option<bool>,
    ) -> Self {
        KafkaSourceParams {
            bootstrap_servers,
            group_id,
            topic,
            enable_partition_eof,
        }
    }
}

pub struct KafkaSourceFactory;

#[async_trait]
impl TypedSourceFactory for KafkaSourceFactory {
    type Source = KafkaSource;
    type Params = KafkaSourceParams;

    async fn typed_create_source(
        params: KafkaSourceParams,
        checkpoint: Checkpoint,
    ) -> anyhow::Result<Self::Source> {
        KafkaSource::try_new(params, checkpoint).await
    }
}

struct KafkaSourceContext;

impl ClientContext for KafkaSourceContext {}

impl ConsumerContext for KafkaSourceContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        info!("Committing offsets: {:?}", result);
    }
}

type KafkaSourceConsumer = StreamConsumer<KafkaSourceContext>;

#[derive(Default)]
pub struct KafkaSourceState {
    pub current_positions: HashMap<i32, Position>,
    /// Number of partitions that have not reached EOF.
    pub num_active_partitions: usize,
    /// Number of partitions initially assigned to the source.
    pub num_assigned_partitions: usize,
    /// Number of bytes processed by the source.
    pub num_bytes_processed: u64,
    /// Number of messages processed by the source.
    pub num_messages_processed: u64,
}

pub struct KafkaSource {
    bootstrap_servers: String,
    group_id: String,
    topic: String,
    consumer: KafkaSourceConsumer,
    state: KafkaSourceState,
}

impl fmt::Debug for KafkaSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KafkaSource {{ bootstrap_servers: {}, group_id: {}, topic:{} }}",
            self.bootstrap_servers, self.group_id, self.topic
        )
    }
}

impl KafkaSource {
    pub async fn try_new(
        params: KafkaSourceParams,
        checkpoint: Checkpoint,
    ) -> anyhow::Result<KafkaSource> {
        let consumer = create_consumer(
            params.bootstrap_servers.as_str(),
            params.group_id.as_str(),
            params.enable_partition_eof.unwrap_or(false),
        )?;
        let topic = params.topic.as_str();
        let partitions = fetch_partitions(&consumer, topic).await?;
        let watermarks = fetch_watermarks(&consumer, topic, &partitions).await?;
        let kafka_checkpoint = kafka_checkpoint_from_checkpoint(&checkpoint)?;
        let assignment = compute_assignment(topic, &partitions, &kafka_checkpoint, &watermarks)?;

        debug!(
            bootstrap_servers = ?params.bootstrap_servers.as_str(),
            group_id = ?params.group_id.as_str(),
            topic = ?params.topic.as_str(),
            assignment = ?assignment,
            "Starting Kafka source."
        );

        consumer
            .assign(&assignment)
            .context("Failed to resume from checkpoint.")?;

        let state = KafkaSourceState {
            num_active_partitions: partitions.len(),
            num_assigned_partitions: partitions.len(),
            ..Default::default()
        };
        Ok(KafkaSource {
            bootstrap_servers: params.bootstrap_servers,
            group_id: params.group_id,
            topic: params.topic,
            consumer,
            state,
        })
    }

    async fn send_batch(
        &self,
        ctx: &SourceContext,
        batch_sink: &Mailbox<IndexerMessage>,
        docs: Vec<String>,
        checkpoint_delta: CheckpointDelta,
    ) -> Result<(), ActorExitStatus> {
        let batch = RawDocBatch {
            docs,
            checkpoint_delta,
        };
        ctx.send_message(batch_sink, IndexerMessage::from(batch))
            .await?;
        Ok(())
    }

    async fn send_eof(
        &self,
        ctx: &SourceContext,
        batch_sink: &Mailbox<IndexerMessage>,
    ) -> Result<(), ActorExitStatus> {
        info!(topic = &self.topic.as_str(), "Reached end of topic.");
        ctx.send_message(batch_sink, IndexerMessage::EndOfSource)
            .await?;
        Err(ActorExitStatus::Success)
    }
}

#[async_trait]
impl Source for KafkaSource {
    async fn emit_batches(
        &mut self,
        batch_sink: &Mailbox<IndexerMessage>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        let mut docs = Vec::new();
        let mut checkpoint_delta = CheckpointDelta::default();

        let deadline = tokio::time::sleep(quickwit_actors::HEARTBEAT / 2);
        let mut message_stream = Box::pin(self.consumer.stream().take_until(deadline));

        while let Some(message_res) = message_stream.next().await {
            let message = match message_res {
                Ok(message) => message,
                Err(KafkaError::PartitionEOF(partition)) => {
                    self.state.num_active_partitions -= 1;
                    info!(
                        topic = ?self.topic.as_str(),
                        partition = ?partition,
                        num_active_partitions = ?self.state.num_active_partitions,
                        "Reached end of partition."
                    );
                    continue;
                }
                // FIXME: This is assuming that Kafka errors are not recoverable, it may not be the case.
                Err(err) => return Err(ActorExitStatus::from(anyhow::anyhow!(err))),
            };
            debug!(
                topic = ?message.topic(),
                partition = ?message.partition(),
                offset = ?message.offset(),
                timestamp = ?message.timestamp(),
                num_bytes = ?message.payload_len(),
                "Message received.",
            );
            if let Some(doc) = parse_message_payload(&message) {
                docs.push(doc);
                self.state.num_bytes_processed += message.payload_len() as u64;
                self.state.num_messages_processed += 1;
            }
            let partition_id = PartitionId::from(message.partition());
            let current_position = Position::from(message.offset());
            let previous_position = self
                .state
                .current_positions
                .insert(message.partition(), current_position.clone())
                .unwrap_or_else(|| previous_position(message.offset()));
            checkpoint_delta
                .record_partition_delta(partition_id, previous_position, current_position)
                .context("Failed to record partition delta.")?;
        }
        if checkpoint_delta.len() > 0 {
            self.send_batch(ctx, batch_sink, docs, checkpoint_delta)
                .await?;
        }
        if self.state.num_active_partitions == 0 {
            self.send_eof(ctx, batch_sink).await?;
        }
        Ok(())
    }

    fn observable_state(&self) -> serde_json::Value {
        json!({
            "group_id": self.group_id,
            "topic":  self.topic,
            // "current_positions": self.state.current_positions,
            "num_active_partitions": self.state.num_active_partitions,
            "num_assigned_partitions": self.state.num_assigned_partitions,
            "num_bytes_processed": self.state.num_bytes_processed,
            "num_messages_processed": self.state.num_messages_processed,
        })
    }
}

fn previous_position(offset: i64) -> Position {
    if offset == 0 {
        Position::Beginning
    } else {
        Position::from(offset - 1)
    }
}

fn create_consumer(
    bootstrap_servers: &str,
    group_id: &str,
    enable_partition_eof: bool,
) -> anyhow::Result<KafkaSourceConsumer> {
    let consumer: KafkaSourceConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id)
        .set("enable.partition.eof", enable_partition_eof.to_string())
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        //.set("statistics.interval.ms", "30000")
        //.set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Info)
        .create_with_context(KafkaSourceContext)
        .with_context(|| {
            format!(
                "Failed to create consumer with bootstrap servers `{}` and group ID `{}`.",
                bootstrap_servers, group_id,
            )
        })?;
    Ok(consumer)
}

/// Converts a checkpoint ...
fn kafka_checkpoint_from_checkpoint(checkpoint: &Checkpoint) -> anyhow::Result<HashMap<i32, i64>> {
    let mut kafka_checkpoint = HashMap::with_capacity(checkpoint.len());

    for (partition_id, position) in checkpoint.iter() {
        let partition_i32 = partition_id.0.parse::<i32>().with_context(|| {
            format!("Failed to parse partition ID `{}` to i32.", partition_id.0)
        })?;
        let offset_i64 = match position {
            Position::Beginning => continue,
            Position::Offset(offset_str) => offset_str
                .parse::<i64>()
                .with_context(|| format!("Failed to parse offset `{}` to i64.", offset_str))?,
        };
        kafka_checkpoint.insert(partition_i32, offset_i64);
    }
    Ok(kafka_checkpoint)
}

/// Fetches the partition the list of partition IDs for a given topic.
async fn fetch_partitions(consumer: &KafkaSourceConsumer, topic: &str) -> anyhow::Result<Vec<i32>> {
    let cluster_metadata = async move {
        let timeout = Timeout::After(Duration::from_secs(5));
        consumer
            .fetch_metadata(Some(topic), timeout)
            .with_context(|| format!("Failed to fetch metadata for topic `{}`.", topic))
    }
    .await?;

    if cluster_metadata.topics().is_empty() {
        bail!("Topic `{}` does not exist.", topic);
    }
    let topic_metadata = &cluster_metadata.topics()[0];
    assert!(topic_metadata.name() == topic); // Belt and suspenders.

    Ok(topic_metadata
        .partitions()
        .iter()
        .map(|partition| partition.id())
        .collect())
}

/// Fetches the low and high watermarks for a given topic and list of partitions.
async fn fetch_watermarks(
    consumer: &KafkaSourceConsumer,
    topic: &str,
    partitions: &[i32],
) -> anyhow::Result<HashMap<i32, (i64, i64)>> {
    let timeout = Duration::from_secs(10);
    let tasks = partitions.iter().map(|&partition| async move {
        consumer
            .fetch_watermarks(topic, partition, timeout)
            .map(|watermarks| (partition, watermarks))
            .with_context(|| {
                format!(
                    "Failed to fetch watermarks for topic `{}` and partition `{}`.",
                    topic, partition
                )
            })
    });
    let watermarks = futures::future::try_join_all(tasks).await?;
    Ok(watermarks.into_iter().collect())
}

/// Computes ...
fn compute_assignment(
    topic: &str,
    partitions: &[i32],
    checkpoint: &HashMap<i32, i64>,
    watermarks: &HashMap<i32, (i64, i64)>,
) -> anyhow::Result<TopicPartitionList> {
    let mut assignment = TopicPartitionList::with_capacity(partitions.len());
    for &partition in partitions {
        let next_offset = compute_next_offset(partition, checkpoint, watermarks)?;
        let _ = assignment.add_partition_offset(topic, partition, next_offset)?;
    }
    Ok(assignment)
}

/// Computes ...
fn compute_next_offset(
    partition: i32,
    checkpoint: &HashMap<i32, i64>,
    watermarks: &HashMap<i32, (i64, i64)>,
) -> anyhow::Result<Offset> {
    let checkpoint_offset = match checkpoint.get(&partition) {
        Some(&checkpoint_offset) => checkpoint_offset,
        None => return Ok(Offset::Beginning),
    };
    let (low_watermark, high_watermark) = match watermarks.get(&partition) {
        Some(&watermarks) => watermarks,
        None => bail!("Missing watermarks for partition `{}`.", partition),
    };
    // We found a gap between the last checkpoint and the low watermark, so we resume from the low watermark.
    if checkpoint_offset < low_watermark {
        return Ok(Offset::Offset(low_watermark));
    }
    // This is the happy path, we resume right after the last checkpointed offset.
    if checkpoint_offset < high_watermark {
        return Ok(Offset::Offset(checkpoint_offset + 1));
    }
    // We start consuming messages from the end of the partition.
    if checkpoint_offset == high_watermark {
        return Ok(Offset::End);
    }
    bail!(
        "Last checkpointed offset `{}` is greater than current high watermark `{}`.",
        checkpoint_offset,
        high_watermark
    );
}

fn parse_message_payload(message: &BorrowedMessage) -> Option<String> {
    match message.payload_view::<str>() {
        Some(Ok(payload)) if payload.len() > 0 => return Some(payload.to_string()),
        Some(Ok(_)) => debug!(
            topic = ?message.topic(),
            partition = ?message.partition(),
            offset = ?message.offset(),
            timestamp = ?message.timestamp(),
            "Document is empty."
        ),
        Some(Err(err)) => warn!(
            topic = ?message.topic(),
            partition = ?message.partition(),
            offset = ?message.offset(),
            timestamp = ?message.timestamp(),
            error = ?err,
            "Failed to deserialize message payload."
        ),
        None => debug!(
            topic = ?message.topic(),
            partition = ?message.partition(),
            offset = ?message.offset(),
            timestamp = ?message.timestamp(),
            "Message payload is empty."
        ),
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::SourceActor;
    use quickwit_actors::create_test_mailbox;
    use quickwit_actors::Universe;
    use rand::distributions::Alphanumeric;
    use rand::Rng;
    use rdkafka::admin::TopicReplication;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::message::ToBytes;
    use rdkafka::producer::FutureProducer;
    use rdkafka::producer::FutureRecord;

    fn append_random_suffix(string: &str) -> String {
        let rng = rand::thread_rng();
        let slug: String = rng
            .sample_iter(&Alphanumeric)
            .take(4)
            .map(char::from)
            .collect();
        format!("{}-{}", string, slug)
    }

    fn create_admin_client(
        bootstrap_servers: &str,
    ) -> anyhow::Result<AdminClient<DefaultClientContext>> {
        let admin_client = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .create()?;
        Ok(admin_client)
    }

    async fn create_topic(
        admin_client: &AdminClient<DefaultClientContext>,
        topic: &str,
        num_partitions: i32,
    ) -> anyhow::Result<()> {
        admin_client
            .create_topics(
                &[NewTopic::new(
                    topic,
                    num_partitions,
                    TopicReplication::Fixed(1),
                )],
                &AdminOptions::new().operation_timeout(Some(Duration::from_secs(5))),
            )
            .await?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|(topic, err_code)| {
                anyhow::anyhow!(
                    "Failed to create topic `{}`. Error code: `{}`",
                    topic,
                    err_code
                )
            })?;
        Ok(())
    }

    async fn populate_topic<K, M, J, Q>(
        bootstrap_servers: &str,
        topic: &str,
        num_messages: i32,
        key_fn: &K,
        message_fn: &M,
        partition: Option<i32>,
        timestamp: Option<i64>,
    ) -> anyhow::Result<HashMap<(i32, i64), i32>>
    where
        K: Fn(i32) -> J,
        M: Fn(i32) -> Q,
        J: ToBytes,
        Q: ToBytes,
    {
        let producer: &FutureProducer = &ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("statistics.interval.ms", "500")
            .set("api.version.request", "true")
            .set("debug", "all")
            .set("message.timeout.ms", "30000")
            .create()?;
        let tasks = (0..num_messages).map(|id| async move {
            producer
                .send(
                    FutureRecord {
                        topic,
                        partition,
                        timestamp,
                        key: Some(&key_fn(id)),
                        payload: Some(&message_fn(id)),
                        headers: None,
                    },
                    Duration::from_secs(1),
                )
                .await
                .map(|(partition, offset)| (id, partition, offset))
                .map_err(|(err, _)| err)
        });
        let message_map = futures::future::try_join_all(tasks)
            .await?
            .into_iter()
            .fold(HashMap::new(), |mut acc, (id, partition, offset)| {
                acc.insert((partition, offset), id);
                acc
            });
        Ok(message_map)
    }

    pub fn key_fn(id: i32) -> String {
        format!("Key {}", id)
    }

    pub fn message_fn(id: i32) -> String {
        format!("Message #{}", id)
    }

    #[tokio::test]
    async fn test_kafka_source() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();

        let universe = Universe::new();

        let bootstrap_servers = "localhost:9092".to_string();
        let group_id = append_random_suffix("test-kafka-source-consumer-group");
        let topic = append_random_suffix("test-kafka-source-topic");

        let admin_client = create_admin_client(&bootstrap_servers)?;
        create_topic(&admin_client, &topic, 3).await?;

        let params = KafkaSourceParams {
            bootstrap_servers: bootstrap_servers.clone(),
            group_id: group_id.clone(),
            topic: topic.clone(),
            enable_partition_eof: Some(true),
        };
        {
            let (sink, inbox) = create_test_mailbox();
            let checkpoint = Checkpoint::default();
            let source = KafkaSource::try_new(params.clone(), checkpoint).await?;
            let actor = SourceActor {
                source: Box::new(source),
                batch_sink: sink.clone(),
            };
            let (_mailbox, handle) = universe.spawn_async_actor(actor);

            let (exit_status, exit_state) = handle.join().await;
            assert!(exit_status.is_success());

            let messages = inbox.drain_available_message_for_test();
            assert_eq!(messages.len(), 1);
            assert!(matches!(messages[0], IndexerMessage::EndOfSource));

            let expected_state = json!({
                "group_id": group_id,
                "topic":  topic,
                // "current_positions":  {},
                "num_active_partitions": 0,
                "num_assigned_partitions": 3,
                "num_bytes_processed": 0,
                "num_messages_processed": 0,
            });
            assert_eq!(exit_state, expected_state);
        }
        for partition in 0..3 {
            populate_topic(
                &bootstrap_servers,
                &topic,
                2,
                &key_fn,
                &|message_id| format!("Message #{:0>3}", partition * 100 + message_id),
                Some(partition),
                None,
            )
            .await?;
        }
        {
            let (sink, inbox) = create_test_mailbox();
            let checkpoint = Checkpoint::default();
            let source = KafkaSource::try_new(params.clone(), checkpoint).await?;
            let actor = SourceActor {
                source: Box::new(source),
                batch_sink: sink.clone(),
            };
            let (_mailbox, handle) = universe.spawn_async_actor(actor);

            let (exit_status, counters) = handle.join().await;
            assert!(exit_status.is_success());

            let mut messages = inbox.drain_available_message_for_test();
            assert_eq!(messages.len(), 2);
            assert!(matches!(messages[0], IndexerMessage::Batch(_)));
            assert!(matches!(messages[1], IndexerMessage::EndOfSource));

            if let IndexerMessage::Batch(batch) = &mut messages[0] {
                batch.docs.sort();
                let expected_docs = vec![
                    "Message #000",
                    "Message #001",
                    "Message #100",
                    "Message #101",
                    "Message #200",
                    "Message #201",
                ];
                assert_eq!(batch.docs, expected_docs);

                let mut expected_checkpoint_delta = CheckpointDelta::default();
                for partition in 0..3 {
                    expected_checkpoint_delta.record_partition_delta(
                        PartitionId::from(partition),
                        Position::Beginning,
                        Position::from(1u64),
                    )?;
                }
                assert_eq!(batch.checkpoint_delta, expected_checkpoint_delta);
            };

            let expected_counters = json!({
                "group_id": group_id,
                "topic":  topic,
                // "current_positions":  {},
                "num_active_partitions": 0,
                "num_assigned_partitions": 3,
                "num_bytes_processed": 72,
                "num_messages_processed": 6,
            });
            assert_eq!(counters, expected_counters);
        }
        {
            let (sink, inbox) = create_test_mailbox();
            let checkpoint: Checkpoint = vec![(0, 0), (1, 1)].into();
            let source = KafkaSource::try_new(params.clone(), checkpoint).await?;
            let actor = SourceActor {
                source: Box::new(source),
                batch_sink: sink.clone(),
            };
            let (_mailbox, handle) = universe.spawn_async_actor(actor);

            let (exit_status, counters) = handle.join().await;
            assert!(exit_status.is_success());

            let mut messages = inbox.drain_available_message_for_test();
            assert_eq!(messages.len(), 2);
            assert!(matches!(messages[0], IndexerMessage::Batch(_)));
            assert!(matches!(messages[1], IndexerMessage::EndOfSource));

            if let IndexerMessage::Batch(batch) = &mut messages[0] {
                batch.docs.sort();
                let expected_docs = vec!["Message #001", "Message #200", "Message #201"];
                assert_eq!(batch.docs, expected_docs);

                let mut expected_checkpoint_delta = CheckpointDelta::default();
                expected_checkpoint_delta.record_partition_delta(
                    PartitionId::from(0),
                    Position::from(0u64),
                    Position::from(1u64),
                )?;
                expected_checkpoint_delta.record_partition_delta(
                    PartitionId::from(2),
                    Position::Beginning,
                    Position::from(1u64),
                )?;
                assert_eq!(batch.checkpoint_delta, expected_checkpoint_delta,);
            };
            let expected_counters = json!({
                "group_id": group_id,
                "topic":  topic,
                // "current_positions":  {},
                "num_active_partitions": 0,
                "num_assigned_partitions": 3,
                "num_bytes_processed": 36,
                "num_messages_processed": 3,
            });
            assert_eq!(counters, expected_counters);
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_partitions() -> anyhow::Result<()> {
        let bootstrap_servers = "localhost:9092".to_string();
        let group_id = append_random_suffix("test-fetch-partitions-consumer-group");
        let topic = append_random_suffix("test-fetch-partitions-topic");

        let admin_client = create_admin_client(&bootstrap_servers)?;
        create_topic(&admin_client, &topic, 2).await?;

        let consumer = create_consumer(&bootstrap_servers, &group_id, true)?;
        let partitions = fetch_partitions(&consumer, &topic).await?;
        assert_eq!(&partitions, &[0, 1]);
        Ok(())
    }

    #[tokio::test]
    async fn test_fetch_watermarks() -> anyhow::Result<()> {
        let bootstrap_servers = "localhost:9092".to_string();
        let group_id = append_random_suffix("test-fetch-watermarks-consumer-group");
        let topic = append_random_suffix("test-fetch-watermarks-topic");

        let admin_client = create_admin_client(&bootstrap_servers)?;
        create_topic(&admin_client, &topic, 2).await?;

        let consumer = create_consumer(&bootstrap_servers, &group_id, true)?;
        // Force metadata update for the consumer. Otherwise, `fetch_watermarks` may return `UnknownPartition`
        // if the broker hasn't received a metadata update since the topic was created.
        // See also https://issues.apache.org/jira/browse/KAFKA-6829.
        consumer.fetch_metadata(Some(&topic), Duration::from_secs(5))?;
        {
            let watermarks = fetch_watermarks(&consumer, &topic, &[0, 1, 2]).await?;
            let expected_watermarks = vec![(0, (0, 0)), (1, (0, 0))].into_iter().collect();
            assert_eq!(watermarks, expected_watermarks);
        }
        for partition in 0..2 {
            populate_topic(
                &bootstrap_servers,
                &topic,
                1,
                &key_fn,
                &message_fn,
                Some(partition),
                None,
            )
            .await?;
        }
        {
            let watermarks = fetch_watermarks(&consumer, &topic, &[0, 1]).await?;
            let expected_watermarks = vec![(0, (0, 1)), (1, (0, 1))].into_iter().collect();
            assert_eq!(watermarks, expected_watermarks);
        }
        for partition in 0..2 {
            populate_topic(
                &bootstrap_servers,
                &topic,
                1,
                &key_fn,
                &message_fn,
                Some(partition),
                None,
            )
            .await?;
        }
        {
            let watermarks = fetch_watermarks(&consumer, &topic, &[0, 1, 2]).await?;
            let expected_watermarks = vec![(0, (0, 2)), (1, (0, 2))].into_iter().collect();
            assert_eq!(watermarks, expected_watermarks);
        }
        Ok(())
    }

    #[test]
    fn test_compute_assignment() -> anyhow::Result<()> {
        let partitions = &[0, 1, 2];
        let checkpoint = vec![(1, 99), (2, 1337)].into_iter().collect();
        let watermarks = vec![(1, (66, 99)), (2, (1789, 2048))].into_iter().collect();
        let assignment = compute_assignment("topic", partitions, &checkpoint, &watermarks)?;
        let partitions = assignment.elements();
        assert_eq!(partitions.len(), 3);
        assert!(partitions
            .iter()
            .all(|partition| partition.topic() == "topic"));
        assert!(partitions
            .iter()
            .enumerate()
            .all(|(idx, partition)| partition.partition() == idx as i32));
        assert_eq!(partitions[0].offset(), Offset::Beginning);
        assert_eq!(partitions[1].offset(), Offset::End);
        assert_eq!(partitions[2].offset(), Offset::Offset(1789));
        Ok(())
    }

    #[test]
    fn test_compute_next_offset() -> anyhow::Result<()> {
        {
            let checkpoint = HashMap::new();
            let watermarks = HashMap::new();
            let next_offset = compute_next_offset(0, &checkpoint, &watermarks)?;
            assert_eq!(next_offset, Offset::Beginning);
        }
        {
            let checkpoint = vec![(0, 0)].into_iter().collect();
            let watermarks = vec![(0, (5, 10))].into_iter().collect();
            let next_offset = compute_next_offset(0, &checkpoint, &watermarks)?;
            assert_eq!(next_offset, Offset::Offset(5));
        }
        {
            let checkpoint = vec![(0, 4)].into_iter().collect();
            let watermarks = vec![(0, (5, 10))].into_iter().collect();
            let next_offset = compute_next_offset(0, &checkpoint, &watermarks)?;
            assert_eq!(next_offset, Offset::Offset(5));
        }
        {
            let checkpoint = vec![(0, 5)].into_iter().collect();
            let watermarks = vec![(0, (5, 10))].into_iter().collect();
            let next_offset = compute_next_offset(0, &checkpoint, &watermarks)?;
            assert_eq!(next_offset, Offset::Offset(6));
        }
        {
            let checkpoint = vec![(0, 7)].into_iter().collect();
            let watermarks = vec![(0, (5, 10))].into_iter().collect();
            let next_offset = compute_next_offset(0, &checkpoint, &watermarks)?;
            assert_eq!(next_offset, Offset::Offset(8));
        }
        {
            let checkpoint = vec![(0, 9)].into_iter().collect();
            let watermarks = vec![(0, (5, 10))].into_iter().collect();
            let next_offset = compute_next_offset(0, &checkpoint, &watermarks)?;
            assert_eq!(next_offset, Offset::Offset(10));
        }
        {
            let checkpoint = vec![(0, 10)].into_iter().collect();
            let watermarks = vec![(0, (5, 10))].into_iter().collect();
            let next_offset = compute_next_offset(0, &checkpoint, &watermarks)?;
            assert_eq!(next_offset, Offset::End);
        }
        {
            let checkpoint = vec![(0, 0)].into_iter().collect();
            let watermarks = HashMap::new();
            let next_offset = compute_next_offset(0, &checkpoint, &watermarks);
            assert!(next_offset.is_err());
        }
        {
            let checkpoint = vec![(0, 99)].into_iter().collect();
            let watermarks = vec![(0, (5, 10))].into_iter().collect();
            let next_offset = compute_next_offset(0, &checkpoint, &watermarks);
            assert!(next_offset.is_err());
        }
        Ok(())
    }
}
