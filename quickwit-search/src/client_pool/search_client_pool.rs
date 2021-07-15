/*
 * Copyright (C) 2021 Quickwit Inc.
 *
 * Quickwit is offered under the AGPL v3.0 and as commercial software.
 * For commercial licensing, contact us at hello@quickwit.io.
 *
 * AGPL:
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

use std::cmp::Ordering;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;
use tonic::transport::Channel;
use tracing::*;

use quickwit_cluster::cluster::Cluster;
use quickwit_proto::search_service_client::SearchServiceClient;

use crate::client::create_search_service_client;
use crate::client_pool::{ClientPool, Job};
use crate::rendezvous_hasher::{sort_by_rendez_vous_hash, Node};
use crate::swim_addr_to_grpc_addr;

/// Search client pool implementation.
#[derive(Clone)]
pub struct SearchClientPool {
    /// Search clients.
    /// A hash map with gRPC's SocketAddr as the key and SearchServiceClient as the value.
    /// It is not the cluster listen address.
    pub clients: Arc<RwLock<HashMap<SocketAddr, SearchServiceClient<Channel>>>>,
}

impl SearchClientPool {
    /// Create a search client pool given a cluster.
    /// When a client pool is created, the thread that monitors cluster members
    /// will be started at the same time.
    pub async fn new(cluster: Arc<Cluster>) -> anyhow::Result<Self> {
        let mut clients = HashMap::new();

        // Initialize the client pool with members of the cluster.
        for member in cluster.members() {
            let grpc_addr = swim_addr_to_grpc_addr(member.listen_addr);
            match create_search_service_client(grpc_addr).await {
                Ok(client) => {
                    debug!(grpc_addr=?grpc_addr, "Add a new client to connect to the members of the cluster.");
                    clients.insert(grpc_addr, client);
                }
                Err(err) => {
                    error!(grpc_addr=?grpc_addr, err=?err, "Failed to create search client.")
                }
            };
        }

        // Create search client pool.
        let client_pool = SearchClientPool {
            clients: Arc::new(RwLock::new(clients)),
        };

        // Prepare to start a thread that will monitor cluster members.
        let thread_clients = Arc::clone(&client_pool.clients);
        let mut members_watch_channel = cluster.member_change_watcher();

        // Start to monitor the cluster members.
        tokio::spawn(async move {
            while let Some(members) = members_watch_channel.next().await {
                let mut clients = thread_clients.write().await;

                // Create a list of addresses to be removed.
                let members_addresses: HashSet<SocketAddr> = members
                    .iter()
                    .map(|member| swim_addr_to_grpc_addr(member.listen_addr))
                    .collect();
                let addrs_to_remove: Vec<SocketAddr> = clients
                    .keys()
                    .filter(|socket_addr| !members_addresses.contains(*socket_addr))
                    .cloned()
                    .collect();

                // Remove clients from the client pool.
                for grpc_addr in addrs_to_remove {
                    let removed = clients.remove(&grpc_addr).is_some();
                    if removed {
                        debug!(grpc_addr=?grpc_addr, "Remove a client that is connecting to the node that has been downed or left the cluster.");
                    }
                }

                // Add clients to the client pool.
                for member in members {
                    let grpc_addr = swim_addr_to_grpc_addr(member.listen_addr);
                    if let Entry::Vacant(_entry) = clients.entry(grpc_addr) {
                        match create_search_service_client(grpc_addr).await {
                            Ok(client) => {
                                debug!(grpc_addr=?grpc_addr, "Add a new client that is connecting to the node that has been joined the cluster.");
                                clients.insert(grpc_addr, client);
                            }
                            Err(err) => {
                                error!(grpc_addr=?grpc_addr, err=?err, "Failed to create search client.")
                            }
                        };
                    }
                }
            }
        });

        Ok(client_pool)
    }
}

#[async_trait]
impl ClientPool for SearchClientPool {
    /// Assign the given job to the clients.
    /// Returns a list of pair (SocketAddr, Vec<Job>)
    async fn assign_jobs(
        &self,
        mut jobs: Vec<Job>,
    ) -> anyhow::Result<Vec<(SearchServiceClient<Channel>, Vec<Job>)>> {
        let mut splits_groups: HashMap<SocketAddr, Vec<Job>> = HashMap::new();

        // Distribute using rendez-vous hashing
        let mut nodes: Vec<Node> = Vec::new();
        let mut socket_to_client: HashMap<SocketAddr, SearchServiceClient<Channel>> =
            Default::default();

        {
            // restricting the lock guard lifetime.

            // TODO optimize the case where there are few jobs and many clients.
            let clients = self.clients.read().await;

            for (grpc_addr, client) in clients.iter() {
                let node = Node::new(*grpc_addr, 0);
                nodes.push(node);
                socket_to_client.insert(*grpc_addr, client.clone());
            }
        }

        // Sort job
        jobs.sort_by(|left, right| {
            let cost_ord = right.cost.cmp(&left.cost);
            if cost_ord != Ordering::Equal {
                return cost_ord;
            }
            left.split.cmp(&right.split)
        });

        for job in jobs {
            sort_by_rendez_vous_hash(&mut nodes, &job.split);
            // choose one of the the first two nodes based on least loaded
            let chosen_node_index: usize = if nodes.len() >= 2 {
                if nodes[0].load > nodes[1].load {
                    1
                } else {
                    0
                }
            } else {
                0
            };

            // update node load for next round
            nodes[chosen_node_index].load += job.cost as u64;

            let chosen_leaf_grpc_addr: SocketAddr = nodes[chosen_node_index].peer_grpc_addr;
            splits_groups
                .entry(chosen_leaf_grpc_addr)
                .or_insert_with(Vec::new)
                .push(job);
        }

        let mut client_to_jobs = Vec::new();
        for (socket_addr, jobs) in splits_groups {
            if let Some(client) = socket_to_client.remove(&socket_addr) {
                client_to_jobs.push((client, jobs));
            } else {
                error!("Missing client. This should never happen! Please report");
            }
        }
        Ok(client_to_jobs)
    }
}

#[cfg(test)]
mod tests {
    use crate::client_pool::search_client_pool::create_search_service_client;
    use std::net::{SocketAddr, TcpListener};
    use std::sync::Arc;
    use std::thread;
    use std::time;

    use quickwit_cluster::cluster::{read_host_key, Cluster};
    use quickwit_common::to_socket_addr;

    use crate::client_pool::{ClientPool, Job};
    use crate::swim_addr_to_grpc_addr;
    use crate::SearchClientPool;

    #[tokio::test]
    async fn test_search_client_pool_single_node() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let host_key_path = tmp_dir.path().join("host_key");
        let host_key = read_host_key(host_key_path.as_path())?;

        let tmp_swim_port = available_port()?;
        let swim_addr_string = format!("127.0.0.1:{}", tmp_swim_port);
        let swim_addr = to_socket_addr(&swim_addr_string)?;

        let cluster = Arc::new(Cluster::new(host_key, swim_addr)?);

        let client_pool = Arc::new(SearchClientPool::new(cluster.clone()).await?);

        let clients = client_pool.clients.read().await;

        let mut addrs: Vec<SocketAddr> = clients
            .clone()
            .into_iter()
            .map(|(addr, _client)| addr)
            .collect();
        addrs.sort_by_key(|addr| addr.to_string());
        println!("addrs={:?}", addrs);

        let mut expected = vec![swim_addr_to_grpc_addr(swim_addr)];
        expected.sort_by_key(|addr| addr.to_string());
        println!("expected={:?}", expected);

        assert_eq!(addrs, expected);

        cluster.leave();

        tmp_dir.close()?;

        Ok(())
    }

    #[tokio::test]
    async fn test_search_client_pool_multiple_nodes() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;

        let host_key_path1 = tmp_dir.path().join("host_key1");
        let host_key1 = read_host_key(host_key_path1.as_path())?;
        let tmp_swim_port1 = available_port()?;
        let swim_addr_string1 = format!("127.0.0.1:{}", tmp_swim_port1);
        let swim_addr1 = to_socket_addr(&swim_addr_string1)?;
        let cluster1 = Arc::new(Cluster::new(host_key1, swim_addr1)?);

        let host_key_path2 = tmp_dir.path().join("host_key2");
        let host_key2 = read_host_key(host_key_path2.as_path())?;
        let tmp_swim_port2 = available_port()?;
        let swim_addr_string2 = format!("127.0.0.1:{}", tmp_swim_port2);
        let swim_addr2 = to_socket_addr(&swim_addr_string2)?;
        let cluster2 = Arc::new(Cluster::new(host_key2, swim_addr2)?);
        cluster2.add_peer_node(swim_addr1);

        // Wait for the cluster to be configured.
        thread::sleep(time::Duration::from_secs(5));

        let client_pool1 = Arc::new(SearchClientPool::new(cluster1.clone()).await?);

        let clients1 = client_pool1.clients.read().await;

        let mut addrs: Vec<SocketAddr> = clients1
            .clone()
            .into_iter()
            .map(|(addr, _client)| addr)
            .collect();
        addrs.sort_by_key(|addr| addr.to_string());
        println!("addrs={:?}", addrs);

        let mut expected = vec![
            swim_addr_to_grpc_addr(swim_addr1),
            swim_addr_to_grpc_addr(swim_addr2),
        ];
        expected.sort_by_key(|addr| addr.to_string());
        println!("expected={:?}", expected);

        assert_eq!(addrs, expected);

        cluster1.leave();
        cluster2.leave();

        tmp_dir.close()?;

        Ok(())
    }

    #[tokio::test]
    async fn test_search_client_pool_single_node_assign_jobs() -> anyhow::Result<()> {
        let tmp_dir = tempfile::tempdir()?;
        let host_key_path = tmp_dir.path().join("host_key");
        let host_key = read_host_key(host_key_path.as_path())?;

        let tmp_swim_port = available_port()?;
        let swim_addr_string = format!("127.0.0.1:{}", tmp_swim_port);
        let swim_addr = to_socket_addr(&swim_addr_string)?;

        let cluster = Arc::new(Cluster::new(host_key, swim_addr)?);

        let client_pool = Arc::new(SearchClientPool::new(cluster.clone()).await?);

        let jobs = vec![
            Job {
                split: "split1".to_string(),
                cost: 1,
            },
            Job {
                split: "split2".to_string(),
                cost: 2,
            },
            Job {
                split: "split3".to_string(),
                cost: 3,
            },
            Job {
                split: "split4".to_string(),
                cost: 4,
            },
        ];

        let assigned_jobs = client_pool.assign_jobs(jobs).await?;
        println!("assigned_jobs={:?}", assigned_jobs);

        let expected = vec![(
            create_search_service_client(swim_addr_to_grpc_addr(swim_addr)).await?,
            vec![
                Job {
                    split: "split4".to_string(),
                    cost: 4,
                },
                Job {
                    split: "split3".to_string(),
                    cost: 3,
                },
                Job {
                    split: "split2".to_string(),
                    cost: 2,
                },
                Job {
                    split: "split1".to_string(),
                    cost: 1,
                },
            ],
        )];
        println!("expected={:?}", expected);

        // compare jobs
        assert_eq!(assigned_jobs.get(0).unwrap().1, expected.get(0).unwrap().1);

        cluster.leave();

        tmp_dir.close()?;

        Ok(())
    }

    fn available_port() -> anyhow::Result<u16> {
        match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => Ok(listener.local_addr().unwrap().port()),
            Err(e) => anyhow::bail!(e),
        }
    }
}
