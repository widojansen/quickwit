use crate::Mailbox;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::task::spawn;
use tokio::time;

#[derive(Debug, Clone, Copy)]
pub struct Tick;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Idle,
    Running,
    Terminated,
}

#[derive(Debug, Clone, Copy)]
pub enum Command {
    Terminate,
    Run,
    Pause,
}

pub struct ClockController {
    state: Arc<RwLock<State>>,
}

impl ClockController {
    pub async fn run_command(&self, command: Command) {
        let mut wlock = self.state.write().await;
        *wlock = apply_command(*wlock, command);
    }

    pub async fn run(&self) {
        self.run_command(Command::Run).await;
    }

    pub async fn pause(&self) {
        self.run_command(Command::Pause).await;
    }

    pub async fn terminate(&self) {
        self.run_command(Command::Terminate).await;
    }

    pub async fn state(&self) -> State {
        self.state.read().await.clone()
    }
}

#[must_use]
pub fn clock(duration: Duration, mailbox: Mailbox<Tick>) -> ClockController {
    let state = Arc::new(RwLock::new(State::Idle));
    let state_weak = Arc::downgrade(&state);
    spawn(async move {
        let mut interval = time::interval(duration);
        loop {
            interval.tick().await;
            if let Some(state) = state_weak.upgrade() {
                let state_snapshot = state.read().await.clone();
                match state_snapshot {
                    State::Idle => {}
                    State::Running => {
                        if mailbox.send_async(Tick).await.is_err() {
                            let mut wlock = state.write().await;
                            *wlock = State::Terminated;
                            return;
                        }
                    }
                    State::Terminated => {
                        return;
                    }
                }
            } else {
                return;
            }
        }
    });
    ClockController { state }
}

fn apply_command(state: State, command: Command) -> State {
    match (state, command) {
        (State::Terminated, _) => State::Terminated,
        (_, Command::Terminate) => State::Terminated,
        (_, Command::Pause) => State::Idle,
        (_, Command::Run) => State::Running,
    }
}

#[cfg(test)]
mod tests {
    use std::mem;
    use std::time::Duration;
    use tokio::time;

    use crate::clock::State;

    #[tokio::test]
    async fn test_clock_starts_idle() {
        let (mailbox, debug_inbox) = crate::mock_mailbox();
        let clock_controller = super::clock(Duration::from_millis(1), mailbox);
        assert_eq!(clock_controller.state().await, State::Idle);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(debug_inbox.drain().len(), 0);
    }

    #[tokio::test]
    async fn test_clock_run_pause_run() {
        let (mailbox, debug_inbox) = crate::mock_mailbox();
        let clock_controller = super::clock(Duration::from_millis(1), mailbox);
        clock_controller.run().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        clock_controller.pause().await;
        let num_ticks = debug_inbox.drain().len();
        assert!(num_ticks >= 8 && num_ticks <= 12);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(debug_inbox.drain().len() == 0);
        clock_controller.run().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        let num_ticks = debug_inbox.drain().len();
        assert!(num_ticks >= 18 && num_ticks <= 22);
    }

    #[tokio::test]
    async fn test_drop_controller_should_stop_clock() {
        let (mailbox, debug_inbox) = crate::mock_mailbox();
        let clock_controller = super::clock(Duration::from_millis(1), mailbox);
        clock_controller.run().await;
        time::sleep(Duration::from_millis(5)).await;
        mem::drop(clock_controller);
        let num_ticks = debug_inbox.drain().len();
        assert!(num_ticks >= 3 && num_ticks <= 7);
        tokio::time::sleep(Duration::from_millis(5)).await;
        let num_ticks = debug_inbox.drain().len();
        assert_eq!(num_ticks, 0);
    }
}
