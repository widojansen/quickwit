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

use std::{hash::{Hash, Hasher}, time::Duration};

use crate::{models::{IndexId, SourceId}, sources::SourceParams};
use serde_json;

/// Represents one of the source that should be indexed.
#[derive(Clone, Debug)]
pub struct SourceIndexingConfig {
    /// source_id must be unique. We rely on this for Eq and Hash.
    pub source_id: SourceId,
    pub source_params: SourceParams,
    pub index_id: IndexId,
    /// Period at which the source should be scheduled for indexing.
    /// A lower indexing period has a direct impact on the time to search.
    pub indexing_period: Duration,
    pub metastore_uri: String,
    pub storage_uri: String,
    pub source_type: String,
}

impl Hash for SourceIndexingConfig {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.source_id.hash(state)
    }
}

impl PartialEq for SourceIndexingConfig {
    fn eq(&self, other: &Self) -> bool {
        self.source_id.eq(&other.source_id)
    }
}

impl Eq for SourceIndexingConfig {}
