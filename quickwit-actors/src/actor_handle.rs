use flume::Receiver;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use uuid::Uuid;

use crate::{KillSwitch, Observation, Progress, SendError};

pub struct Mailbox<Message> {
    sender: flume::Sender<ActorMessage<Message>>,
    id: Uuid,
    actor_name: String,
}

impl<Message> Clone for Mailbox<Message> {
    fn clone(&self) -> Self {
        Mailbox {
            sender: self.sender.clone(),
            id: self.id.clone(),
            actor_name: self.actor_name.clone()
        }

    }
}

impl<Message> Mailbox<Message> {
    pub(crate) fn new(sender: flume::Sender<ActorMessage<Message>>, actor_name: String) -> Self {
        Mailbox {
            sender,
            id: Uuid::new_v4(),
            actor_name,
        }
    }

    pub fn actor_name(&self) -> String {
        format!("{}:{}", self.actor_name, self.id)
    }
}

impl<Message> fmt::Debug for Mailbox<Message> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Mailbox({})", self.actor_name())
    }
}

impl<Message> Hash for Mailbox<Message> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state)
    }
}

impl<Message> PartialEq for Mailbox<Message> {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

impl<Message> Eq for Mailbox<Message> {}

impl<Message> Mailbox<Message> {
    pub(crate) async fn send_actor_message(
        &self,
        msg: ActorMessage<Message>,
    ) -> Result<(), SendError> {
        self.sender.send_async(msg).await.map_err(|_| SendError)
    }

    /// Send a message to the actor synchronously.
    ///
    /// SendError is returned if the user is already terminated.
    ///
    /// (See also [Self::send_blocking()])
    pub async fn send_async(&self, msg: Message) -> Result<(), SendError> {
        self.send_actor_message(ActorMessage::Message(msg)).await
    }

    /// Send a message to the actor in a blocking fashion.
    /// When possible, prefer using [Self::send_async()].
    ///
    // TODO do we need a version with a deadline?
    pub fn send_blocking(&self, msg: Message) -> Result<(), SendError> {
        self.sender
            .send(ActorMessage::Message(msg))
            .map_err(|_e| SendError)
    }
}

/// An Actor Handle serves as an address to communicate with an actor.
///
/// It is lightweight to clone it.
/// If all actor handles are dropped, the actor does not die right away.
/// It will process all of the message in its mailbox before being terminated.
///
/// Because `ActorHandle`'s generic types are Message and Observable, as opposed
/// to the actor type, `ActorHandle` are interchangeable.
/// It makes it possible to plug different implementations, have actor proxy etc.
pub struct ActorHandle<Message, ObservableState> {
    inner: Arc<InnerActorHandle<Message, ObservableState>>,
}

impl<Message: fmt::Debug, ObservableState> fmt::Debug for ActorHandle<Message, ObservableState> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ActorHandle({})", self.inner.mailbox.actor_name())
    }
}

impl<Message, ObservableState> Clone for ActorHandle<Message, ObservableState> {
    fn clone(&self) -> Self {
        ActorHandle {
            inner: self.inner.clone(),
        }
    }
}

impl<Message: Send + Sync + fmt::Debug, ObservableState: Clone + Send + fmt::Debug>
    ActorHandle<Message, ObservableState>
{
    pub(crate) fn new(
        mailbox: Mailbox<Message>,
        last_state: watch::Receiver<ObservableState>,
        join_handle: JoinHandle<ActorTermination>,
        progress: Progress,
        kill_switch: KillSwitch,
    ) -> Self {
        let mut interval = tokio::time::interval(crate::HEARTBEAT);
        let kill_switch_clone = kill_switch.clone();
        tokio::task::spawn(async move {
            interval.tick().await;
            while kill_switch.is_alive() {
                interval.tick().await;
                if !progress.has_changed() {
                    kill_switch.kill();
                    return;
                }
                progress.reset();
            }
        });
        ActorHandle {
            inner: Arc::new(InnerActorHandle {
                mailbox,
                join_handle,
                kill_switch: kill_switch_clone,
                last_state,
            }),
        }
    }

    /// Returns a snapshot of the observable state of the actor.
    ///
    /// Observe goes through the mechanism of message passing too.
    /// As a result, it actually waits for all of the pending message
    /// in the inbox to be processed before snapshotting.
    ///
    /// Therefore, it can be used in unit test to "sync" the actor,
    /// and some race conditions.
    ///
    /// Because the observation requires to wait for the mailbox to be empty,
    /// observation, it may timeout.
    ///
    /// In that case, [Observation::Timeout] is returned with the last
    /// observed state.
    pub async fn observe(&self) -> Observation<ObservableState> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .inner
            .mailbox
            .send_actor_message(ActorMessage::Observe(tx))
            .await;
        let observable_state_or_timeout = timeout(crate::HEARTBEAT, rx).await;
        let state = self.inner.last_state.borrow().clone();
        match observable_state_or_timeout {
            Ok(Ok(())) => Observation::Running(state),
            Ok(Err(_)) => Observation::Terminated(state),
            Err(_) => {
                if self.inner.kill_switch.is_alive() {
                    Observation::Timeout(state)
                } else {
                    self.inner.join_handle.abort();
                    Observation::Terminated(state)
                }
            }
        }
    }
}

struct InnerActorHandle<Message, ObservableState> {
    mailbox: Mailbox<Message>,
    join_handle: JoinHandle<ActorTermination>,
    kill_switch: KillSwitch,
    last_state: watch::Receiver<ObservableState>,
}

/// Represents the cause of termination of an actor.
pub enum ActorTermination {
    /// Process command returned false.
    OnDemand,
    /// The actor process method returned an error.
    ActorError(anyhow::Error),
    /// The actor was killed by the kill switch.
    KillSwitch,
    /// All of the actor handle were dropped and no more message were available.
    Disconnect,

    DownstreamClosed,
}

pub(crate) enum ActorMessage<Message> {
    Message(Message),
    Observe(oneshot::Sender<()>),
}

impl<Message: fmt::Debug> fmt::Debug for ActorMessage<Message> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(msg) => {
                write!(f, "Message({:?})", msg)
            }
            Self::Observe(_) => {
                write!(f, "Observe")
            }
        }
    }
}

pub struct DebugInbox<M>(Receiver<ActorMessage<M>>);

impl<M> DebugInbox<M> {
    pub fn drain(&self) -> Vec<M> {
        self.0
            .drain()
            .flat_map(|msg| match msg {
                ActorMessage::Message(msg) => Some(msg),
                ActorMessage::Observe(_) => None,
            })
            .collect()
    }
}

pub fn mock_mailbox<M>() -> (Mailbox<M>, DebugInbox<M>) {
    let (tx, rx) = flume::unbounded();
    let mailbox = Mailbox::new(tx, "mock_actor".to_string());
    let debug_inbox = DebugInbox(rx);
    (mailbox, debug_inbox)
}
