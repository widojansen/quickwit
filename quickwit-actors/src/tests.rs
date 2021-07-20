use crate::actor::{Actor, KillSwitch, Progress};
use crate::actor_handle::Mailbox;
use crate::{AsyncActor, MessageProcessError, Observation, SyncActor};
use async_trait::async_trait;
use std::collections::HashSet;

// An actor that receives ping messages.
#[derive(Default)]
pub struct PingReceiverSyncActor {
    ping_count: usize,
}

#[derive(Debug, Clone)]
pub struct Ping;

impl Actor for PingReceiverSyncActor {
    type Message = Ping;

    type ObservableState = usize;

    fn name(&self) -> String {
        "Ping".to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {
        self.ping_count
    }
}

impl SyncActor for PingReceiverSyncActor {
    fn process_message(
        &mut self,
        _message: Self::Message,
        _progress: &Progress,
    ) -> Result<(), MessageProcessError> {
        self.ping_count += 1;
        Ok(())
    }
}

#[derive(Default)]
pub struct PingerAsyncSenderActor {
    count: usize,
    peers: HashSet<Mailbox<<PingReceiverSyncActor as Actor>::Message>>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct SenderState {
    pub count: usize,
    pub num_peers: usize,
}

#[derive(Debug, Clone)]
pub enum SenderMessage {
    AddPeer(Mailbox<Ping>),
    Ping,
}

impl Actor for PingerAsyncSenderActor {
    type Message = SenderMessage;
    type ObservableState = SenderState;

    fn name(&self) -> String {
        "PingSender".to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {
        SenderState {
            count: self.count,
            num_peers: self.peers.len(),
        }
    }
}

#[async_trait]
impl AsyncActor for PingerAsyncSenderActor {
    async fn process_message(
        &mut self,
        message: SenderMessage,
        _progress: &Progress,
    ) -> Result<(), MessageProcessError> {
        match message {
            SenderMessage::AddPeer(peer) => {
                self.peers.insert(peer);
            }
            SenderMessage::Ping => {
                self.count += 1;
                for peer in &self.peers {
                    let _ = peer.send_async(Ping).await;
                }
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_ping_actor() {
    let kill_switch = KillSwitch::default();
    let (ping_recv_mailbox, ping_recv_handle) =
        PingReceiverSyncActor::default().spawn(10, kill_switch.clone());
    let (ping_sender_mailbox, ping_sender_handle) =
        PingerAsyncSenderActor::default().spawn(10, kill_switch.clone());
    assert_eq!(ping_recv_handle.observe().await, Observation::Running(0));
    // No peers. This one will have no impact.
    assert!(ping_sender_mailbox
        .send_async(SenderMessage::Ping)
        .await
        .is_ok());
    assert!(ping_sender_mailbox
        .send_async(SenderMessage::AddPeer(ping_recv_mailbox.clone()))
        .await
        .is_ok());
    assert_eq!(
        ping_sender_handle.observe().await,
        Observation::Running(SenderState {
            num_peers: 1,
            count: 1
        })
    );
    assert!(ping_sender_mailbox
        .send_async(SenderMessage::Ping)
        .await
        .is_ok());
    assert!(ping_sender_mailbox
        .send_async(SenderMessage::Ping)
        .await
        .is_ok());
    assert_eq!(
        ping_sender_handle.observe().await,
        Observation::Running(SenderState {
            num_peers: 1,
            count: 3
        })
    );
    assert_eq!(ping_recv_handle.observe().await, Observation::Running(2));
    kill_switch.kill();
    assert_eq!(ping_recv_handle.observe().await, Observation::Terminated(2));
    assert_eq!(
        ping_sender_handle.observe().await,
        Observation::Terminated(SenderState {
            num_peers: 1,
            count: 3
        })
    );
    assert!(ping_sender_mailbox
        .send_async(SenderMessage::Ping)
        .await
        .is_err());
}

struct BuggyActor;

#[derive(Debug, Clone)]
enum BuggyMessage {
    DoNothing,
    Block,
}

impl Actor for BuggyActor {
    type Message = BuggyMessage;
    type ObservableState = ();

    fn name(&self) -> String {
        "BuggyActor".to_string()
    }

    fn observable_state(&self) -> Self::ObservableState {
        ()
    }
}

#[async_trait]
impl AsyncActor for BuggyActor {
    async fn process_message(
        &mut self,
        message: BuggyMessage,
        _progress: &Progress,
    ) -> Result<(), MessageProcessError> {
        match message {
            BuggyMessage::Block => {
                loop {
                    // we could keep the actor alive by calling `progress.record_progress()` here.
                    tokio::time::sleep(tokio::time::Duration::from_secs(3_600)).await;
                }
            }
            BuggyMessage::DoNothing => {}
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_timeouting_actor() {
    let kill_switch = KillSwitch::default();
    let (buggy_mailbox, buggy_handle) = BuggyActor.spawn(10, kill_switch.clone());
    assert_eq!(buggy_handle.observe().await, Observation::Running(()));
    assert!(buggy_mailbox
        .send_async(BuggyMessage::DoNothing)
        .await
        .is_ok());
    assert_eq!(buggy_handle.observe().await, Observation::Running(()));
    assert!(buggy_mailbox.send_async(BuggyMessage::Block).await.is_ok());
    assert_eq!(buggy_handle.observe().await, Observation::Timeout(()));
    tokio::time::sleep(crate::HEARTBEAT).await;
    tokio::time::sleep(crate::HEARTBEAT).await;
    assert_eq!(buggy_handle.observe().await, Observation::Terminated(()));
}
