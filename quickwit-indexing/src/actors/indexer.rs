use std::sync::Arc;

use anyhow::Context;
use anyhow::bail;
use quickwit_actors::Actor;
use quickwit_actors::Mailbox;
use quickwit_actors::SyncActor;
use quickwit_index_config::IndexConfig;
use tantivy::Document;
use tantivy::Index;
use tantivy::IndexWriter;
use tempfile::TempDir;

use crate::split::Split;

// Quickwit
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

struct Scratch {
    index_writer: IndexWriter,
    tempdir: TempDir,
}

impl Scratch {
    pub fn create(index_config: &dyn IndexConfig, memory_in_bytes: usize) -> anyhow::Result<Scratch> {
        let tempdir = tempfile::tempdir()?;
        let schema = index_config.schema();
        let index = Index::create_in_dir(tempdir.path(), schema)?;
        let index_writer = index.writer_with_num_threads(1, memory_in_bytes)?;
        Ok(Scratch {
            index_writer,
            tempdir,
        })
    }
}

pub struct IndexerParams {
    index: String,
    index_config: Arc<dyn IndexConfig>,
    mem_budget_in_bytes: usize,
}

#[derive(Default, Clone, Debug)]
pub struct IndexerStatistics {
    num_docs: usize,
}

pub struct Indexer {
    params: IndexerParams,
    packager_mailbox: Mailbox<Split>,
    statistics: IndexerStatistics,
    scratch_opt: Option<Scratch>,
}

impl Indexer {
    fn new(params: IndexerParams, packager_mailbox: Mailbox<Split>) -> anyhow::Result<Indexer> {
        let mut indexer = Indexer {
            params,
            packager_mailbox,
            statistics: Default::default(),
            scratch_opt: None,
        };
        indexer.create_scratch();
        Ok(indexer)
    }

    fn create_scratch(&mut self) -> anyhow::Result<()> {
        if self.scratch_opt.is_some() {
            bail!("Scratch already exists.");
        }
        let scratch = Scratch::create(&*self.params.index_config, self.params.mem_budget_in_bytes)?;
        self.scratch_opt = Some(scratch);
        Ok(())
    }

    fn scratch(&self) -> anyhow::Result<&Scratch> {
        if let Some(scratch) = self.scratch_opt.as_ref() {
            Ok(scratch)
        } else {
            bail!("Missing scratch");
        }
    }

}

impl Actor for Indexer {
    type Message = String;

    type ObservableState = IndexerStatistics;

    fn observable_state(&self) -> IndexerStatistics {
        self.statistics.clone()
    }
}

impl SyncActor for Indexer {
    fn process_message(
        &mut self,
        doc_json: String,
        progress: &quickwit_actors::Progress,
    ) -> anyhow::Result<bool> {
        let doc = self.params.index_config.doc_from_json(&doc_json)?;
        let scratch = self.scratch()?;
        scratch.index_writer.add_document(doc);
        Ok(true)
    }

    fn finalize(&mut self) -> anyhow::Result<()> {
        let mut scratch = self.scratch_opt.take()
            .with_context(|| "Missing scratch")?;
        scratch.index_writer.commit()?;
        let split = Split {
            directory: scratch.tempdir,
        };
        self.packager_mailbox.send_blocking(split);
        Ok(())
    }
}


