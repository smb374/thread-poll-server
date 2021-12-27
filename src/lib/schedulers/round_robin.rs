use super::{BoxedFuture, ScheduleMessage, Scheduler, Spawner, Task};

use std::{
    sync::Arc,
    thread::{self, JoinHandle},
};

use crossbeam::channel::{self, Receiver, Select, Sender};
use parking_lot::Mutex;

enum Message {
    Close,
}

pub struct RoundRobinScheduler {
    size: usize,
    current_index: usize,
    threads: Vec<(WorkerInfo, JoinHandle<()>)>,
    tx: Sender<ScheduleMessage>,
    rx: Receiver<ScheduleMessage>,
}

struct WorkerInfo {
    task_tx: Sender<Arc<Task>>,
    tx: Sender<Message>,
}

pub struct Worker {
    _idx: usize,
    task_rx: Receiver<Arc<Task>>,
    rx: Receiver<Message>,
}

impl RoundRobinScheduler {
    fn new(size: usize) -> (Spawner, Self) {
        let (tx, rx) = channel::unbounded();
        let spawner = Spawner::new(tx.clone());
        let threads: Vec<(WorkerInfo, JoinHandle<()>)> = (0..size)
            .map(|_idx| {
                let (tx, rx) = channel::unbounded();
                let (task_tx, task_rx) = channel::unbounded();
                let handle = thread::spawn(move || {
                    let worker = Worker { _idx, task_rx, rx };
                    worker.run();
                });
                (WorkerInfo { task_tx, tx }, handle)
            })
            .collect();
        let scheduler = Self {
            size,
            current_index: 0,
            threads,
            tx,
            rx,
        };
        (spawner, scheduler)
    }
    fn round(&mut self) -> usize {
        let r = self.current_index;
        self.current_index = (self.current_index + 1) % self.size;
        r
    }
}

impl Scheduler for RoundRobinScheduler {
    fn init(size: usize) -> (Spawner, Self) {
        Self::new(size)
    }
    fn schedule(&mut self, future: BoxedFuture) {
        let index = self.round();
        // let tx = &self.threads[index].0.tx;
        let task_tx = &self.threads[index].0.task_tx;
        let task = Arc::new(Task {
            future,
            tx: Mutex::new(Some(task_tx.clone())),
        });
        task_tx.send(task).expect("Failed to send message");
        // tx.send(Message::Run).expect("Failed to send message");
    }
    fn reschedule(&mut self, task: Arc<Task>) {
        let index = self.round();
        // let tx = &self.threads[index].0.tx;
        let task_tx = &self.threads[index].0.task_tx;
        task_tx.send(task).expect("Failed to send message");
        // tx.send(Message::Run).expect("Failed to send message");
    }
    fn shutdown(self) {
        for (info, handle) in self.threads {
            let tx = info.tx;
            tx.send(Message::Close).expect("Failed to send message");
            let _ = handle.join();
        }
    }
    fn receiver(&self) -> &Receiver<ScheduleMessage> {
        &self.rx
    }
}

impl Worker {
    fn run(&self) {
        let mut select = Select::new();
        let task_index = select.recv(&self.task_rx);
        let rx_index = select.recv(&self.rx);
        loop {
            let oper = select.select();
            match oper.index() {
                i if i == task_index => {
                    if let Ok(task) = oper.recv(&self.task_rx) {
                        task.run();
                    } else {
                        break;
                    }
                }
                i if i == rx_index => match oper.recv(&self.rx) {
                    Ok(Message::Close) => break,
                    Err(e) => {
                        eprintln!("recv error: {}", e);
                        break;
                    }
                },
                _ => unreachable!(),
            }
        }
    }
}
