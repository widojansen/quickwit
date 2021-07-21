//  Quickwit
//  Copyright (C) 2021 Quickwit Inc.
//
//  Quickwit is offered under the AGPL v3.0 and as commercial software.
//  For commercial licensing, contact us at hello@quickwit.io.
//
//  AGPL:
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Affero General Public License as
//  published by the Free Software Foundation, either version 3 of the
//  License, or (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Affero General Public License for more details.
//
//  You should have received a copy of the GNU Affero General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

use crate::collector::QuickwitCollector;
use anyhow::Context;
use futures::future::try_join_all;
use itertools::Itertools;
use quickwit_directories::{CachingDirectory, HotDirectory, StorageDirectory, HOTCACHE_FILENAME};
use quickwit_proto::LeafSearchResult;
use quickwit_storage::Storage;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tantivy::{collector::Collector, query::Query, Index, ReloadPolicy, Searcher, Term};
use tokio::task::spawn_blocking;

/// Opens a `tantivy::Index` for the given split.
///
/// The resulting index uses a dynamic and a static cache.
pub(crate) async fn open_index(split_storage: Arc<dyn Storage>) -> anyhow::Result<Index> {
    let hotcache_bytes = split_storage
        .get_all(Path::new(HOTCACHE_FILENAME))
        .await
        .with_context(|| format!("Failed to fetch hotcache from {}", split_storage.uri()))?;
    let directory = StorageDirectory::new(split_storage);
    let caching_directory = CachingDirectory::new_with_unlimited_capacity(Arc::new(directory));
    let hot_directory = HotDirectory::open(caching_directory, hotcache_bytes)?;
    let index = Index::open(hot_directory)?;
    Ok(index)
}

/// Tantivy search does not make it possible to fetch data asynchronously during
/// search.
///
/// It is required to download all required information in advance.
/// This is the role of the `warmup` function.
///
/// The downloaded data depends on the query (which term's posting list is required,
/// are position required too), and the collector.
async fn warmup(
    searcher: &Searcher,
    query: &dyn Query,
    quickwit_collector: &QuickwitCollector,
) -> anyhow::Result<()> {
    warm_up_terms(searcher, query).await?;
    warm_up_fastfields(searcher, quickwit_collector).await?;
    Ok(())
}

async fn warm_up_fastfields(
    searcher: &Searcher,
    quickwit_collector: &QuickwitCollector,
) -> anyhow::Result<()> {
    let mut fast_fields = Vec::new();
    for fast_field_name in quickwit_collector.fast_field_names.iter() {
        let fast_field = searcher
            .schema()
            .get_field(fast_field_name)
            .with_context(|| {
                format!(
                    "Couldn't get field named {:?} from schema.",
                    fast_field_name
                )
            })?;

        let field_entry = searcher.schema().get_field_entry(fast_field);
        if !field_entry.is_fast() {
            anyhow::bail!("Field {:?} is not a fast field.", fast_field_name);
        }
        fast_fields.push(fast_field);
    }

    let mut warm_up_futures = Vec::new();
    for field in fast_fields {
        for segment_reader in searcher.segment_readers() {
            let fast_field_slice = segment_reader.fast_fields().fast_field_data(field, 0)?;
            warm_up_futures.push(async move { fast_field_slice.read_bytes_async().await });
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

async fn warm_up_terms(searcher: &Searcher, query: &dyn Query) -> anyhow::Result<()> {
    let mut terms: BTreeMap<Term, bool> = Default::default();
    query.query_terms(&mut terms);
    let grouped_terms = terms.iter().group_by(|term| term.0.field());
    let mut warm_up_futures = Vec::new();
    for (field, terms) in grouped_terms.into_iter() {
        let terms: Vec<(&Term, bool)> = terms
            .map(|(term, position_needed)| (term, *position_needed))
            .collect();
        for segment_reader in searcher.segment_readers() {
            let inv_idx = segment_reader.inverted_index(field)?;
            for (term, position_needed) in terms.iter().cloned() {
                let inv_idx_clone = inv_idx.clone();
                warm_up_futures
                    .push(async move { inv_idx_clone.warm_postings(&term, position_needed).await });
            }
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

/// Apply a leaf search on a single split.
async fn leaf_search_single_split(
    query: &dyn Query,
    quickwit_collector: QuickwitCollector,
    storage: Arc<dyn Storage>,
) -> anyhow::Result<LeafSearchResult> {
    let index = open_index(storage).await?;
    let reader = index
        .reader_builder()
        .num_searchers(1)
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let searcher = reader.searcher();
    warmup(&*searcher, query, &quickwit_collector).await?;
    let leaf_search_result = searcher.search(query, &quickwit_collector)?;
    Ok(leaf_search_result)
}

/// `leaf` step of search.
///
/// The leaf search collects all kind of information, and returns a set of [PartialHit] candidates.
/// The root will be in charge to consolidate, identify the actual final top hits to display, and
/// fetch the actual documents to convert the partial hits into actual Hits.
pub async fn leaf_search(
    query: &dyn Query,
    collector: QuickwitCollector,
    split_ids: &[String],
    storage: Arc<dyn Storage>,
) -> anyhow::Result<LeafSearchResult> {
    let leaf_search_single_split_futures: Vec<_> = split_ids
        .iter()
        .map(|split_id| {
            let split_storage: Arc<dyn Storage> =
                quickwit_storage::add_prefix_to_storage(storage.clone(), split_id);
            let collector_for_split = collector.for_split(split_id.clone());
            async move { leaf_search_single_split(query, collector_for_split, split_storage).await }
        })
        .collect();
    // TODO avoid failing all for one issue. We could be more resilient on storage failures.
    let split_search_results = futures::future::join_all(leaf_search_single_split_futures).await;

    let (search_results, _errors): (Vec<_>, Vec<_>) =
        split_search_results.into_iter().partition(Result::is_ok);
    let search_results: Vec<_> = search_results.into_iter().map(Result::unwrap).collect();
    //let errors: Vec<_> = errors.into_iter().map(Result::unwrap_err).collect();

    let merged_search_results = spawn_blocking(move || collector.merge_fruits(search_results))
        .await
        .with_context(|| "Merging search on split results failed")??;

    // TODO save error, but switch away from anyhow first
    //merged_search_results.failed_requests.extend(errors.iter());
    Ok(merged_search_results)
}
