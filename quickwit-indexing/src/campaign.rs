use std::sync::Arc;

use quickwit_actors::AsyncActor;
use quickwit_actors::KillSwitch;
use quickwit_metastore::Metastore;
use quickwit_storage::Storage;

use crate::Checkpoint;
use crate::IndexId;
use crate::SourceId;
use crate::actors::Publisher;
use crate::actors::build_source;

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


struct Campaign {
    source_id: SourceId,
    index_id: IndexId,
    storage: Arc<dyn Storage>,
    metastore: Arc<dyn Metastore>,
}

async fn fetch_checkpoint(metastore: &dyn Metastore, source_id: &SourceId) -> anyhow::Result<Checkpoint> {
    todo!()
}

async fn run_campaign(campaign: Campaign) -> anyhow::Result<()> {
    let resume_checkpoint = fetch_checkpoint(&*campaign.metastore, &campaign.source_id).await?;
    let publisher = Publisher::new(campaign.metastore.clone());
    let kill_switch = KillSwitch::default();
    publisher.spawn(3, kill_switch);
    // let writer: Indexer = Indexer(index_id).await?;
    // let packager = build_packager(source, index_writer).await?;
    // let uploader = build_uploader(campaign.storage).await?;
    // let publisher = build_publisher(campaign.storage).await?;
    let source = build_source(&campaign.source_id, resume_checkpoint).await?;
    Ok(())
}
