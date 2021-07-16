use std::any::type_name;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use thiserror::Error;

use crate::SendError;


#[derive(Error, Debug)]
pub enum MessageProcessError {
    #[error("On Demand")]
    OnDemand,
    #[error("Downstream actor closed connection")]
    DownstreamClosed,
    #[error("Failure")]
    Error(#[from] anyhow::Error)
}

impl From<SendError> for MessageProcessError {
    fn from(_: SendError) -> Self {
        MessageProcessError::DownstreamClosed
    }
}

/// An actor has an internal state and processes a stream of message.
///
/// While processing a message, the actor typically
/// - Update its state
/// - emit one or more message to other actors.
///
/// Actors exists in two flavor:
/// - async actors, are executed in event thread in tokio runtime.
/// - sync actors, executed on the blocking thread pool of tokio runtime.
pub trait Actor: Send + Sync + 'static {
    /// Type of message that can be received by the actor.
    type Message: Send + Sync + fmt::Debug;
    /// Piece of state that can be copied for assert in unit test, admin, etc.
    type ObservableState: Send + Clone + Sync + fmt::Debug;
    /// A name identifying the type of actor.
    /// It does not need to be "instance-unique", and can be the name of
    /// the actor implementation.
    fn name(&self) -> String {
        type_name::<Self>().to_string()
    }

    /// Extracts an observable state. Useful for unit test, and admin UI.
    ///
    /// This function should return fast, but it is not called after receiving
    /// single message. Snapshotting happens when the actor is terminated, or
    /// in an on demand fashion by calling `ActorHandle::observe()`.
    fn observable_state(&self) -> Self::ObservableState;
}

/// Makes it possible to register some progress.
///
/// If no progress is observed until the next heartbeat, the actor will be killed.
#[derive(Clone)]
pub struct Progress(Arc<AtomicBool>);

impl Default for Progress {
    fn default() -> Progress {
        Progress(Arc::new(AtomicBool::new(false)))
    }
}

impl Progress {
    pub fn record_progress(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn has_changed(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.0.store(false, Ordering::Relaxed);
    }
}


#[derive(Clone)]
pub struct KillSwitch {
    step_id: usize,
    lowest_step_alive: Arc<AtomicUsize>
}

impl Default for KillSwitch {
    fn default() -> Self {
        KillSwitch {
            step_id: 0,
            lowest_step_alive: Arc::new(AtomicUsize::new(0))
        }
    }
}

impl KillSwitch {
    pub fn kill(&self) {
        self.lowest_step_alive.fetch_max(self.step_id + 1, Ordering::AcqRel);
    }

    pub fn is_alive(&self) -> bool {
        let lowest_step_alive = self.lowest_step_alive.load(Ordering::Relaxed);
        lowest_step_alive < self.step_id
    }

    pub fn add_step(&self) -> KillSwitch {
        KillSwitch {
            step_id: self.step_id + 1,
            lowest_step_alive: self.lowest_step_alive.clone(),
        }
    }
}
