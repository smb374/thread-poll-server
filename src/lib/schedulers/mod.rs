pub mod round_robin;
pub mod work_stealing;

use std::ops::DerefMut;
use std::sync::Arc;

use crossbeam::channel::{Receiver, Sender};
use futures_lite::future::{Boxed, Future, FutureExt};
use futures_task::{waker_ref, ArcWake, Context, Poll};
use parking_lot::Mutex;

pub enum ScheduleMessage {
    Schedule(BoxedFuture),
    Reschedule(Arc<Task>),
    Shutdown,
}

pub struct Spawner {
    tx: Sender<ScheduleMessage>,
}

pub trait Scheduler {
    fn init(size: usize) -> (Spawner, Self);
    fn schedule(&mut self, future: BoxedFuture);
    fn reschedule(&mut self, task: Arc<Task>);
    fn shutdown(self);
    fn receiver(&self) -> &Receiver<ScheduleMessage>;
}

pub type BoxedFuture = Mutex<Option<Boxed<()>>>;

pub struct Task {
    future: BoxedFuture,
    tx: Mutex<Option<Sender<Arc<Task>>>>,
}

impl Spawner {
    pub fn new(tx: Sender<ScheduleMessage>) -> Self {
        Self { tx }
    }
    pub fn spawn<F>(&self, future: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let boxed = Mutex::new(Some(future.boxed()));
        self.tx
            .send(ScheduleMessage::Schedule(boxed))
            .expect("Failed to send message");
    }
    pub fn shutdown(&self) {
        self.tx
            .send(ScheduleMessage::Shutdown)
            .expect("Failed to send message");
    }
}

impl Task {
    pub fn run(self: &Arc<Self>) {
        let mut guard = self.future.lock();
        let future_slot = guard.deref_mut();
        // run *ONCE*
        if let Some(mut future) = future_slot.take() {
            let waker = waker_ref(self);
            let cx = &mut Context::from_waker(&waker);
            match future.as_mut().poll(cx) {
                Poll::Ready(r) => return r,
                Poll::Pending => {
                    *future_slot = Some(future);
                }
            };
        }
    }
    pub fn replace_tx(&self, tx: Sender<Arc<Task>>) {
        let mut guard = self.tx.lock();
        guard.deref_mut().replace(tx);
    }
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let clone = Arc::clone(arc_self);
        let guard = arc_self.tx.lock();
        guard
            .as_ref()
            .expect("task's tx should be assigned when scheduled at the first time")
            .send(clone)
            .expect("Too many message queued!");
    }
}
