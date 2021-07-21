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

// TODO Handle hot change in the scheduler config.

use tokio::task::JoinHandle;

use crate::scheduling::scheduler_config::SchedulerConfig;
use crate::scheduling::unique_queue::{unbounded_channel, Sender};
use crate::scheduling::SourceIndexingConfig;

pub fn start_scheduler(config: SchedulerConfig) -> JoinHandle<()> {
    let (task_queue_tx, mut task_queue_rx) = unbounded_channel::<SourceIndexingConfig>();
    for source_config in config.source_configs {
        let join = tokio::task::spawn(queue_tasks_periodically(
            task_queue_tx.clone(),
            source_config,
        ));
    }
    tokio::task::spawn(async move {
        while let Some(source_config) = task_queue_rx.recv().await {
            println!("{:?}", source_config);
        }
    })
}

async fn queue_tasks_periodically(
    mut task_queue_tx: Sender<SourceIndexingConfig>,
    source_indexing_config: SourceIndexingConfig,
) {
    println!("queuetasksperiod");
    let mut interval = tokio::time::interval(source_indexing_config.indexing_period);
    loop {
        interval.tick().await;
        if task_queue_tx.send(source_indexing_config.clone()).await.is_err() {
            // The scheduling task has stopped.
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::scheduling::SourceIndexingConfig;

    #[tokio::test]
    async fn test_task_queues() {
        let source_config_1 = SourceIndexingConfig {
            source_id: "source1".to_string(),
            index_id: "index1".to_string(),
            period: Duration::from_millis(100),
            metastore_uri: "".to_string(),
            storage_uri: "".to_string(),
        };
        let source_config_2 = SourceIndexingConfig {
            source_id: "source2".to_string(),
            index_id: "index2".to_string(),
            period: Duration::from_millis(300),
            metastore_uri: "".to_string(),
            storage_uri: "".to_string(),
        };
        let scheduler_config = SchedulerConfig {
            source_configs: vec![source_config_1, source_config_2],
        };
        /*
        let join_handle = start_scheduler(scheduler_config);
        join_handle.await;
        */
    }
}
