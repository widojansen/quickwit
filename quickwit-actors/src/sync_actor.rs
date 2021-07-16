use tokio::sync::watch;
use tokio::task::spawn_blocking;
use tracing::debug;

use crate::actor::MessageProcessError;
use crate::actor_handle::{ActorMessage, ActorTermination, Mailbox};
use crate::{Actor, ActorHandle, KillSwitch, Progress};

/// An sync actor is executed on a tokio blocking task.
///
/// It may block and perform CPU heavy computation.
/// (See also [`AsyncActor`])
pub trait SyncActor: Actor + Sized {
    /// Processes a message.
    ///
    /// If true is returned, the actors will continue processing messages.
    /// If false is returned, the actor will terminate "gracefully".
    ///
    /// If an error is returned, the actor will be killed, as well as all of the actor
    /// under the same kill switch.
    fn process_message(
        &mut self,
        message: Self::Message,
        progress: &Progress,
    ) -> Result<(), MessageProcessError>;

    /// Function called if there are no more messages available.
    fn finalize(&mut self) -> anyhow::Result<()> { Ok(()) }


    #[doc(hidden)]
    fn spawn(
        mut self,
        message_queue_limit: usize,
        kill_switch: KillSwitch,
    ) -> (
        Mailbox<Self::Message>,
        ActorHandle<Self::Message, Self::ObservableState>,
    ) {
        let actor_name = self.name();
        let (sender, receiver) = flume::bounded::<ActorMessage<Self::Message>>(message_queue_limit);
        let (state_tx, state_rx) = watch::channel(self.observable_state());
        let progress = Progress::default();
        let progress_clone = progress.clone();
        let kill_switch_clone = kill_switch.clone();
        let join_handle = spawn_blocking::<_, ActorTermination>(move || {
            let actor_name = self.name();
            let termination =
                sync_actor_loop(&mut self, receiver, &state_tx, kill_switch, progress);
            debug!("Termination of actor {}", actor_name);
            let _ = state_tx.send(self.observable_state());
            termination
        });
        let mailbox = Mailbox::new(sender, actor_name);
        let actor_handle = ActorHandle::new(
            mailbox.clone(),
            state_rx,
            join_handle,
            progress_clone,
            kill_switch_clone,
        );
        (mailbox, actor_handle)
    }
}

fn sync_actor_loop<A: SyncActor>(
    actor: &mut A,
    inbox: flume::Receiver<ActorMessage<A::Message>>,
    state_tx: &watch::Sender<A::ObservableState>,
    kill_switch: KillSwitch,
    progress: Progress,
) -> ActorTermination {
    loop {
        if !kill_switch.is_alive() {
            return ActorTermination::KillSwitch;
        }
        progress.record_progress();
        let sync_msg_res = inbox.recv_timeout(crate::HEARTBEAT.mul_f32(0.2));
        progress.record_progress();
        if !kill_switch.is_alive() {
            return ActorTermination::KillSwitch;
        }
        match sync_msg_res {
            Ok(ActorMessage::Message(message)) => {
                match actor.process_message(message, &progress) {
                    Ok(()) => (),
                    Err(MessageProcessError::OnDemand) => {
                        return ActorTermination::OnDemand
                    },
                    Err(MessageProcessError::Error(err)) => {
                        kill_switch.kill();
                        return ActorTermination::ActorError(err);
                    }
                    Err(MessageProcessError::DownstreamClosed) => {
                        kill_switch.kill();
                        return ActorTermination::DownstreamClosed;
                    }
                }
            }
            Ok(ActorMessage::Observe(oneshot)) => {
                let state = actor.observable_state();
                // We voluntarily ignore the error here. (An error only occurs if the
                // sender dropped its receiver.)
                let _ = state_tx.send(state);
                let _ = oneshot.send(());
            }
            Err(flume::RecvTimeoutError::Disconnected) => {
                if let Err(actor_error) = actor.finalize() {
                    return ActorTermination::ActorError(actor_error);
                }
                return ActorTermination::Disconnect;
            }
            Err(flume::RecvTimeoutError::Timeout) => {
                // This is just a timeout.
                continue;
            }
        }
    }
}
