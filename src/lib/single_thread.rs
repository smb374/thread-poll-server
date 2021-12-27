use super::reactor;
use futures_lite::future::{Boxed, FutureExt};
use futures_task::{waker_ref, ArcWake};
use once_cell::sync::Lazy;
use std::{
    future::Future,
    io,
    sync::{
        mpsc::{self, Receiver, SyncSender},
        Arc, Mutex,
    },
    task::{Context, Poll},
};

static SPAWNER: Lazy<Mutex<Option<Spawner>>> = Lazy::new(|| Mutex::new(None));

struct Task {
    future: Mutex<Option<Boxed<io::Result<()>>>>,
    tx: SyncSender<Message>,
}

enum Message {
    Run(Arc<Task>),
    Close,
}

pub struct Executor {
    rx: Receiver<Message>,
}

struct Spawner {
    tx: SyncSender<Message>,
}

impl Executor {
    pub fn new(channel_size: usize) -> Self {
        let (tx, rx) = mpsc::sync_channel(channel_size);
        let spawner = Spawner { tx };
        SPAWNER.lock().unwrap().replace(spawner);
        Self { rx }
    }

    fn run(&self) -> io::Result<()> {
        loop {
            match self.rx.try_recv() {
                Ok(msg) => match msg {
                    // run task
                    Message::Run(task) => task.run()?,
                    // received disconnect message, cleanup and exit.
                    Message::Close => break Ok(()),
                },
                Err(mpsc::TryRecvError::Empty) => {
                    // mio wait for io harvest
                    // reactor::wait(None)?;
                }
                // no one is connected, bye.
                Err(mpsc::TryRecvError::Disconnected) => break Ok(()),
            }
        }
    }
    pub fn block_on<F>(&self, future: F) -> io::Result<()>
    where
        F: Future<Output = io::Result<()>> + 'static + Send,
    {
        spawn(future);
        self.run()
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        if let Some(spawner) = SPAWNER.lock().unwrap().as_ref() {
            spawner
                .tx
                .send(Message::Close)
                .expect("Message queue is full.");
        }
    }
}

impl Task {
    pub fn run(self: &Arc<Self>) -> io::Result<()> {
        let mut future_slot = self.future.lock().unwrap();
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
        Ok(())
    }
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        let clone = Arc::clone(arc_self);
        arc_self
            .tx
            .send(Message::Run(clone))
            .expect("Too many message queued!");
    }
}

impl Spawner {
    fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = io::Result<()>> + 'static + Send,
    {
        let future = fut.boxed();
        let task = Arc::new(Task {
            future: Mutex::new(Some(future)),
            tx: self.tx.clone(),
        });
        self.tx
            .send(Message::Run(task))
            .expect("too many task queued");
    }
}

pub fn spawn<F>(fut: F)
where
    F: Future<Output = io::Result<()>> + 'static + Send,
{
    if let Some(spawner) = SPAWNER.lock().unwrap().as_ref() {
        spawner.spawn(fut);
    }
}
