use super::reactor;
use super::schedulers::{ScheduleMessage, Scheduler, Spawner};

use std::{
    ops::{Deref, DerefMut},
    thread::{self, JoinHandle},
};

use crossbeam::channel::{self, Receiver, Sender, TryRecvError};
use futures_lite::prelude::*;
use once_cell::sync::Lazy;
use parking_lot::Mutex;

static SPAWNER: Lazy<Mutex<Option<Spawner>>> = Lazy::new(|| Mutex::new(None));

pub struct Executor<S: Scheduler> {
    scheduler: S,
    poll_thread_notifier: Sender<()>,
    poll_thread_handle: JoinHandle<()>,
}

impl<S: Scheduler> Executor<S> {
    pub fn new() -> Self {
        let cpus = num_cpus::get();
        let size = if cpus == 0 { 1 } else { cpus - 1 };
        let (spawner, scheduler) = S::init(size);
        let (tx, rx) = channel::unbounded();
        // set up spawner
        SPAWNER.lock().deref_mut().replace(spawner);
        let poll_thread_handle = thread::spawn(move || Self::poll_thread(rx));
        Self {
            scheduler,
            poll_thread_notifier: tx,
            poll_thread_handle,
        }
    }
    fn poll_thread(rx: Receiver<()>) {
        let mut reactor = reactor::Reactor::new();
        reactor.setup_registry();
        loop {
            match rx.try_recv() {
                // exit signal
                Ok(()) | Err(TryRecvError::Disconnected) => break,
                Err(TryRecvError::Empty) => {}
            };
            if let Err(e) = reactor.wait(None) {
                eprintln!("reactor wait error: {}, exit poll thread", e);
                break;
            }
        }
    }
    fn run(mut self) {
        loop {
            match self.scheduler.receiver().recv() {
                // continously schedule tasks
                Ok(msg) => match msg {
                    ScheduleMessage::Schedule(future) => self.scheduler.schedule(future),
                    ScheduleMessage::Reschedule(task) => self.scheduler.reschedule(task),
                    ScheduleMessage::Shutdown => break,
                },
                Err(_) => {
                    eprintln!("exit...");
                    break;
                }
            }
        }
        // shutdown worker threads
        self.scheduler.shutdown();
        // shutdown poll thread
        self.poll_thread_notifier
            .send(())
            .expect("Failed to send notify");
        let _ = self.poll_thread_handle.join();
    }
    pub fn block_on<F>(self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        spawn(future);
        self.run();
    }
}

pub fn spawn<F>(future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    if let Some(spawner) = SPAWNER.lock().deref() {
        spawner.spawn(future);
    }
}

pub fn shutdown() {
    if let Some(spawner) = SPAWNER.lock().deref() {
        spawner.shutdown();
    }
}
