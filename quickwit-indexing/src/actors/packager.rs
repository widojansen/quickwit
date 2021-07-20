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

use std::path::Path;

use crate::models::IndexedSplit;
use crate::models::Manifest;
use crate::models::PackagedSplit;
use quickwit_actors::Actor;
use quickwit_actors::Mailbox;
use quickwit_actors::SyncActor;

pub struct Packager {
    pub uploader_mailbox: Mailbox<PackagedSplit>,
}

impl Packager {
    fn package(&self, _index_path: &Path) -> anyhow::Result<Manifest> {
        Ok(Manifest::default())
    }
}

impl Actor for Packager {
    type Message = IndexedSplit;

    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {
        ()
    }
}

impl SyncActor for Packager {
    fn process_message(
        &mut self,
        indexed_split: IndexedSplit,
        _progress: &quickwit_actors::Progress,
    ) -> Result<(), quickwit_actors::MessageProcessError> {
        let manifest = self.package(indexed_split.directory.path())?;
        self.uploader_mailbox.send_blocking(PackagedSplit {
            manifest,
            split_label: indexed_split.label,
            directory: indexed_split.directory,
        })?;
        Ok(())
    }
}
