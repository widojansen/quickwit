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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use itertools::Itertools;
use tantivy::collector::Collector;
use tantivy::TantivyError;
use tokio::task::spawn_blocking;
use tokio::task::JoinHandle;
use tracing::*;

use quickwit_metastore::{Metastore, SplitMetadata};
use quickwit_proto::{
    FetchDocsRequest, FetchDocsResult, Hit, LeafSearchRequest, LeafSearchResult, PartialHit,
    SearchRequest, SearchResult,
};

use crate::client_pool::Job;
use crate::list_relevant_splits;
use crate::make_collector;
use crate::ClientPool;
use crate::SearchClientPool;
use crate::SearchError;
use crate::SearchServiceClient;

// Measure the cost associated to searching in a given split metadata.
fn compute_split_cost(_split_metadata: &SplitMetadata) -> u32 {
    //TODO: Have a smarter cost, by smoothing the number of docs.
    1
}

#[derive(Debug)]
pub struct NodeSearchError {
    search_error: SearchError,
    split_ids: Vec<String>,
}

type SearchResultsByAddr = HashMap<SocketAddr, Result<LeafSearchResult, NodeSearchError>>;

async fn execute_search(
    assigned_leaf_search_jobs: &[(SearchServiceClient, Vec<Job>)],
    search_request_with_offset_0: SearchRequest,
) -> anyhow::Result<SearchResultsByAddr> {
    // Perform the query phase.
    let mut result_per_node_addr_futures = HashMap::new();

    // Perform the query phase.
    for (search_client, jobs) in assigned_leaf_search_jobs.iter() {
        let leaf_search_request = LeafSearchRequest {
            search_request: Some(search_request_with_offset_0.clone()),
            split_ids: jobs.iter().map(|job| job.split.clone()).collect(),
        };

        debug!(leaf_search_request=?leaf_search_request, grpc_addr=?search_client.grpc_addr(), "Leaf node search.");
        let mut search_client_clone: SearchServiceClient = search_client.clone();
        let handle = tokio::spawn(async move {
            let split_ids = leaf_search_request.split_ids.to_vec();
            search_client_clone
                .leaf_search(leaf_search_request)
                .await
                .map_err(|search_error| NodeSearchError {
                    search_error,
                    split_ids,
                })
        });
        result_per_node_addr_futures.insert(search_client.grpc_addr(), handle);
    }
    let mut result_per_node_addr = HashMap::new();
    for (addr, search_result) in result_per_node_addr_futures {
        ////< An error here means that the tokio task panicked... Not that the grpc erorred.
        //< The latter is handled later.
        result_per_node_addr.insert(addr, search_result.await?);
    }

    Ok(result_per_node_addr)
}

pub struct ErrorRetries {
    #[allow(dead_code)]
    retry_split_ids: Vec<String>,
    #[allow(dead_code)]
    nodes_to_use: Vec<SocketAddr>,
}

/// There are different information which could be useful
/// Scenario: Multiple requests on a node
/// - Are all requests of one node failing? -> Don't consider this node as alternative of failing
/// requests
/// - Is the node ok, but just the split failing?
/// - Did all requests fail? (Should we retry in that case?)
fn analyze_errors(search_result: &SearchResultsByAddr) -> Option<ErrorRetries> {
    let contains_errors = search_result.values().any(|res| {
        res.is_err()
            || res
                .as_ref()
                .map(|ok_res| ok_res.failed_requests.is_empty())
                .unwrap_or(false)
    });
    if !contains_errors {
        return None;
    }
    let complete_failure_nodes: Vec<_> = search_result
        .iter()
        .filter(|(_addr, result)| {
            if let Ok(ok_res) = result {
                ok_res.failed_requests.len() as u64 == ok_res.num_attempted_splits
            } else {
                true
            }
        })
        .map(|(addr, _)| *addr)
        .collect();
    let partial_or_no_failure_nodes: Vec<_> = search_result
        .iter()
        .filter(|(_addr, result)| {
            result
                .as_ref()
                .map(|ok_res| ok_res.failed_requests.len() as u64 != ok_res.num_attempted_splits)
                .unwrap_or(false)
        })
        .map(|(addr, _)| *addr)
        .collect();

    // Here we collect the failed requests on the node. It does not yet include failed requests
    // against that node
    let failed_split_ids = search_result
        .values()
        .filter_map(|result| result.as_ref().ok())
        .flat_map(|res| {
            let results = res
                .failed_requests
                .iter()
                .map(move |failed_req| failed_req.split_id.to_string())
                .collect_vec();
            results.into_iter()
        })
        .collect_vec();

    let retry_nodes = if partial_or_no_failure_nodes.is_empty() {
        complete_failure_nodes
    } else {
        partial_or_no_failure_nodes
    };

    Some(ErrorRetries {
        retry_split_ids: failed_split_ids,
        nodes_to_use: retry_nodes,
    })
}

/// Perform a distributed search.
/// It sends a search request over gRPC to multiple leaf nodes and merges the search results.
pub async fn root_search(
    search_request: &SearchRequest,
    metastore: &dyn Metastore,
    client_pool: &Arc<SearchClientPool>,
) -> Result<SearchResult, SearchError> {
    let start_instant = tokio::time::Instant::now();

    // Create a job for leaf node search and assign the splits that the node is responsible for based on the job.
    let split_metadata_list = list_relevant_splits(search_request, metastore).await?;

    // Create a hash map of SplitMetadata with split id as a key.
    let split_metadata_map: HashMap<String, SplitMetadata> = split_metadata_list
        .into_iter()
        .map(|split_metadata| (split_metadata.split_id.clone(), split_metadata))
        .collect();

    // Create a job for fetching docs and assign the splits that the node is responsible for based on the job.
    let leaf_search_jobs: Vec<Job> = split_metadata_map
        .keys()
        .map(|split_id| {
            // TODO: Change to a better way that does not use unwrap().
            let split_metadata = split_metadata_map.get(split_id).unwrap();
            Job {
                split: split_id.clone(),
                cost: compute_split_cost(split_metadata),
            }
        })
        .collect();

    let assigned_leaf_search_jobs = client_pool.assign_jobs(leaf_search_jobs).await?;
    debug!(assigned_leaf_search_jobs=?assigned_leaf_search_jobs, "Assigned leaf search jobs.");

    // Create search request with start offset is 0 that used by leaf node search.
    let mut search_request_with_offset_0 = search_request.clone();
    search_request_with_offset_0.start_offset = 0;
    search_request_with_offset_0.max_hits += search_request.start_offset;

    // Perform the query phase.
    let result_per_node_addr = execute_search(
        &assigned_leaf_search_jobs,
        search_request_with_offset_0.clone(),
    )
    .await?;

    analyze_errors(&result_per_node_addr);

    let index_metadata = metastore.index_metadata(&search_request.index_id).await?;
    let index_config = index_metadata.index_config;
    let collector = make_collector(index_config.as_ref(), search_request);

    // Find the sum of the number of hits and merge multiple partial hits into a single partial hits.
    let mut leaf_search_results = Vec::new();
    for (_addr, leaf_search_response) in result_per_node_addr.into_iter() {
        match leaf_search_response {
            Ok(leaf_search_result) => {
                debug!(leaf_search_result=?leaf_search_result, "Leaf search result.");
                leaf_search_results.push(leaf_search_result)
            }
            Err(node_search_error) => {
                error!(error=?node_search_error, "Leaf request failed");
                // TODO list failed leaf nodes and retry.
                return Err(node_search_error.search_error);
            }
        }
    }

    let leaf_search_result = spawn_blocking(move || collector.merge_fruits(leaf_search_results))
        .await?
        .map_err(|merge_error: TantivyError| {
            crate::SearchError::InternalError(format!("{}", merge_error))
        })?;
    debug!(leaf_search_result=?leaf_search_result, "Merged leaf search result.");

    // Create a hash map of PartialHit with split as a key.
    let mut partial_hits_map: HashMap<String, Vec<PartialHit>> = HashMap::new();
    for partial_hit in leaf_search_result.partial_hits.iter() {
        partial_hits_map
            .entry(partial_hit.split_id.clone())
            .or_insert_with(Vec::new)
            .push(partial_hit.clone());
    }

    // Perform the fetch docs phese.
    let mut fetch_docs_handles: Vec<JoinHandle<Result<FetchDocsResult, SearchError>>> = Vec::new();
    for (search_client, jobs) in assigned_leaf_search_jobs.iter() {
        for job in jobs {
            // TODO group fetch doc requests.
            if let Some(partial_hits) = partial_hits_map.get(&job.split) {
                let fetch_docs_request = FetchDocsRequest {
                    partial_hits: partial_hits.clone(),
                    index_id: search_request.index_id.clone(),
                };
                let mut search_client_clone = search_client.clone();
                let handle = tokio::spawn(async move {
                    search_client_clone.fetch_docs(fetch_docs_request).await
                });
                fetch_docs_handles.push(handle);
            }
        }
    }
    let fetch_docs_responses = futures::future::try_join_all(fetch_docs_handles).await?;

    // Merge the fetched docs.
    let mut hits: Vec<Hit> = Vec::new();
    for response in fetch_docs_responses {
        match response {
            Ok(fetch_docs_result) => {
                hits.extend(fetch_docs_result.hits);
            }
            // TODO handle failure.
            Err(err) => error!(err=?err),
        }
    }
    hits.sort_by(|hit1, hit2| {
        let value1 = if let Some(partial_hit) = &hit1.partial_hit {
            partial_hit.sorting_field_value
        } else {
            0
        };
        let value2 = if let Some(partial_hit) = &hit2.partial_hit {
            partial_hit.sorting_field_value
        } else {
            0
        };
        // Sort by descending order.
        value2.cmp(&value1)
    });

    let elapsed = start_instant.elapsed();

    Ok(SearchResult {
        num_hits: leaf_search_result.num_hits,
        hits,
        elapsed_time_micros: elapsed.as_micros() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::ops::Range;

    use quickwit_index_config::WikipediaIndexConfig;
    use quickwit_metastore::{IndexMetadata, MockMetastore, SplitState};

    use crate::MockSearchService;

    #[tokio::test]
    async fn test_root_search_single_split() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
        };
        println!("search_request={:?}", search_request);

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Box::new(WikipediaIndexConfig::new()),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>| {
                Ok(vec![SplitMetadata {
                    split_id: "split1".to_string(),
                    split_state: SplitState::Published,
                    num_records: 10,
                    size_in_bytes: 256,
                    time_range: None,
                    generation: 1,
                }])
            },
        );

        let mut mock_search_service = MockSearchService::new();
        mock_search_service.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResult {
                    num_hits: 3,
                    partial_hits: vec![
                        quickwit_proto::PartialHit {
                            sorting_field_value: 3,
                            split_id: "split1".to_string(),
                            segment_ord: 1,
                            doc_id: 1,
                        },
                        quickwit_proto::PartialHit {
                            sorting_field_value: 2,
                            split_id: "split1".to_string(),
                            segment_ord: 2,
                            doc_id: 2,
                        },
                        quickwit_proto::PartialHit {
                            sorting_field_value: 1,
                            split_id: "split1".to_string(),
                            segment_ord: 3,
                            doc_id: 3,
                        },
                    ],
                    failed_requests: Vec::new(),
                    num_attempted_splits: 0,
                })
            },
        );
        mock_search_service.expect_fetch_docs().returning(
            |_fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResult {
                    hits: vec![
                        quickwit_proto::Hit {
                            json: "{\"title\" : \"1\", \"body\" : \"test 1\", \"url\" : \"http://127.0.0.1/1\"}".to_string(),
                            partial_hit: Some(quickwit_proto::PartialHit {
                                sorting_field_value: 3,
                                split_id: "split1".to_string(),
                                segment_ord: 1,
                                doc_id: 1,
                            }),
                        },
                        quickwit_proto::Hit {
                            json: "{\"title\" : \"2\", \"body\" : \"test 22\", \"url\" : \"http://127.0.0.1/2\"}".to_string(),
                            partial_hit: Some(quickwit_proto::PartialHit {
                                sorting_field_value: 2,
                                split_id: "split1".to_string(),
                                segment_ord: 2,
                                doc_id: 2,
                            }),
                        },
                        quickwit_proto::Hit {
                            json: "{\"title\" : \"3\", \"body\" : \"test 3\", \"url\" : \"http://127.0.0.1/3\"}".to_string(),
                            partial_hit: Some(quickwit_proto::PartialHit {
                                sorting_field_value: 1,
                                split_id: "split1".to_string(),
                                segment_ord: 3,
                                doc_id: 3,
                            }),
                        },
                    ],
                })
            },
        );

        let client_pool =
            Arc::new(SearchClientPool::from_mocks(vec![Arc::new(mock_search_service)]).await?);

        let search_result = root_search(&search_request, &metastore, &client_pool).await?;
        println!("search_result={:?}", search_result);

        assert_eq!(search_result.num_hits, 3);
        assert_eq!(search_result.hits.len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_root_search_multiple_splits() -> anyhow::Result<()> {
        let search_request = quickwit_proto::SearchRequest {
            index_id: "test-idx".to_string(),
            query: "test".to_string(),
            search_fields: vec!["body".to_string()],
            start_timestamp: None,
            end_timestamp: None,
            max_hits: 10,
            start_offset: 0,
        };
        println!("search_request={:?}", search_request);

        let mut metastore = MockMetastore::new();
        metastore
            .expect_index_metadata()
            .returning(|_index_id: &str| {
                Ok(IndexMetadata {
                    index_id: "test-idx".to_string(),
                    index_uri: "file:///path/to/index/test-idx".to_string(),
                    index_config: Box::new(WikipediaIndexConfig::new()),
                })
            });
        metastore.expect_list_splits().returning(
            |_index_id: &str, _split_state: SplitState, _time_range: Option<Range<i64>>| {
                Ok(vec![
                    SplitMetadata {
                        split_id: "split1".to_string(),
                        split_state: SplitState::Published,
                        num_records: 10,
                        size_in_bytes: 256,
                        time_range: None,
                        generation: 1,
                    },
                    SplitMetadata {
                        split_id: "split2".to_string(),
                        split_state: SplitState::Published,
                        num_records: 10,
                        size_in_bytes: 256,
                        time_range: None,
                        generation: 1,
                    },
                ])
            },
        );

        let mut mock_search_service1 = MockSearchService::new();
        mock_search_service1.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResult {
                    num_hits: 2,
                    partial_hits: vec![
                        quickwit_proto::PartialHit {
                            sorting_field_value: 3,
                            split_id: "split1".to_string(),
                            segment_ord: 1,
                            doc_id: 1,
                        },
                        quickwit_proto::PartialHit {
                            sorting_field_value: 1,
                            split_id: "split1".to_string(),
                            segment_ord: 3,
                            doc_id: 3,
                        },
                    ],
                    failed_requests: Vec::new(),
                    num_attempted_splits: 0,
                })
            },
        );
        mock_search_service1.expect_fetch_docs().returning(
            |_fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResult {
                    hits: vec![
                        quickwit_proto::Hit {
                            json: "{\"title\" : \"1\", \"body\" : \"test 1\", \"url\" : \"http://127.0.0.1/1\"}".to_string(),
                            partial_hit: Some(quickwit_proto::PartialHit {
                                sorting_field_value: 3,
                                split_id: "split1".to_string(),
                                segment_ord: 1,
                                doc_id: 1,
                            }),
                        },
                        quickwit_proto::Hit {
                            json: "{\"title\" : \"3\", \"body\" : \"test 3\", \"url\" : \"http://127.0.0.1/3\"}".to_string(),
                            partial_hit: Some(quickwit_proto::PartialHit {
                                sorting_field_value: 1,
                                split_id: "split1".to_string(),
                                segment_ord: 3,
                                doc_id: 3,
                            }),
                        },
                    ],
                })
            },
        );

        let mut mock_search_service2 = MockSearchService::new();
        mock_search_service2.expect_leaf_search().returning(
            |_leaf_search_req: quickwit_proto::LeafSearchRequest| {
                Ok(quickwit_proto::LeafSearchResult {
                    num_hits: 1,
                    partial_hits: vec![quickwit_proto::PartialHit {
                        sorting_field_value: 2,
                        split_id: "split2".to_string(),
                        segment_ord: 2,
                        doc_id: 2,
                    }],
                    failed_requests: Vec::new(),
                    num_attempted_splits: 0,
                })
            },
        );
        mock_search_service2.expect_fetch_docs().returning(
            |_fetch_docs_req: quickwit_proto::FetchDocsRequest| {
                Ok(quickwit_proto::FetchDocsResult {
                    hits: vec![
                        quickwit_proto::Hit {
                            json: "{\"title\" : \"2\", \"body\" : \"test 22\", \"url\" : \"http://127.0.0.1/2\"}".to_string(),
                            partial_hit: Some(quickwit_proto::PartialHit {
                                sorting_field_value: 2,
                                split_id: "split2".to_string(),
                                segment_ord: 2,
                                doc_id: 2,
                            }),
                        },
                    ],
                })
            },
        );

        let client_pool = Arc::new(
            SearchClientPool::from_mocks(vec![
                Arc::new(mock_search_service1),
                Arc::new(mock_search_service2),
            ])
            .await?,
        );

        let search_result = root_search(&search_request, &metastore, &client_pool).await?;
        println!("search_result={:?}", search_result);

        assert_eq!(search_result.num_hits, 3);
        assert_eq!(search_result.hits.len(), 3);

        Ok(())
    }
}
