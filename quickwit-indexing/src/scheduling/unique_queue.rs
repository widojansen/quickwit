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

use std::collections::HashSet;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::mpsc::{error::SendError, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;

pub struct Receiver<T> {
    items_in_queue: Arc<Mutex<HashSet<T>>>,
    rx: UnboundedReceiver<T>,
}

impl<T: Hash + Eq + PartialEq> Receiver<T> {
    pub async fn recv(&mut self) -> Option<T> {
        let item = self.rx.recv().await?;
        let mut lock = self.items_in_queue.lock().await;
        assert!(lock.remove(&item));
        Some(item)
    }
}

#[derive(Clone)]
pub struct Sender<T> {
    items_in_queue: Arc<Mutex<HashSet<T>>>,
    tx: UnboundedSender<T>,
}

impl<T: Hash + Eq + PartialEq + Clone> Sender<T> {
    pub async fn send(&mut self, item: T) -> Result<(), SendError<T>> {
        let mut lock = self.items_in_queue.lock().await;
        if lock.contains(&item) {
            return Ok(());
        }
        lock.insert(item.clone());
        self.tx.send(item)?;
        Ok(())
    }
}

pub fn unbounded_channel<T>() -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let config_in_queue: Arc<Mutex<HashSet<T>>> = Default::default();
    let receiver = Receiver {
        items_in_queue: config_in_queue.clone(),
        rx,
    };
    let sender = Sender {
        items_in_queue: config_in_queue,
        tx,
    };
    (sender, receiver)
}

#[cfg(test)]
mod tests {
    use super::unbounded_channel;
    use std::mem;

    #[tokio::test]
    async fn test_simple() {
        let (mut tx, mut rx) = unbounded_channel();
        assert!(tx.send(1).await.is_ok());
        assert_eq!(rx.recv().await, Some(1));
        mem::drop(tx);
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn test_unbounce() {
        let (mut tx, mut rx) = unbounded_channel();
        assert!(tx.send(1).await.is_ok());
        assert!(tx.send(1).await.is_ok());
        assert_eq!(rx.recv().await, Some(1));
        mem::drop(tx);
        assert_eq!(rx.recv().await, None);
    }

    #[tokio::test]
    async fn test_unbounce_interleaved() {
        let (mut tx, mut rx) = unbounded_channel();
        assert!(tx.send(1).await.is_ok());
        assert!(tx.send(2).await.is_ok());
        assert!(tx.send(1).await.is_ok());
        assert!(tx.send(3).await.is_ok());
        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
        assert!(tx.send(1).await.is_ok());
        assert_eq!(rx.recv().await, Some(3));
        assert_eq!(rx.recv().await, Some(1));
        mem::drop(tx);
        assert_eq!(rx.recv().await, None);
    }
}
