use super::{BoxedFuture, ScheduleMessage, Scheduler, Spawner, Task};

use std::sync::Arc;

use crossbeam::{
    channel::{self, Receiver, Select, Sender},
    deque::{Injector, Stealer, Worker},
    sync::WaitGroup,
};
use parking_lot::Mutex;

pub struct WorkStealingScheduler {
    _size: usize,
    injector: Arc<Injector<Arc<Task>>>,
    _stealers: Vec<Stealer<Arc<Task>>>,
    wait_group: WaitGroup,
    notifier: Sender<Message>,
    rx: Receiver<ScheduleMessage>,
}

struct TaskRunner {
    _idx: usize,
    worker: Worker<Arc<Task>>,
    injector: Arc<Injector<Arc<Task>>>,
    stealers: Arc<[Stealer<Arc<Task>>]>,
    wait_group: Option<WaitGroup>,
    rx: Receiver<Message>,
    task_tx: Sender<Arc<Task>>,
    task_rx: Receiver<Arc<Task>>,
}

enum Message {
    HaveTasks,
    Close,
}

impl WorkStealingScheduler {
    fn new(size: usize) -> (Spawner, Self) {
        let injector: Arc<Injector<Arc<Task>>> = Arc::new(Injector::new());
        let mut _stealers: Vec<Stealer<Arc<Task>>> = Vec::new();
        let stealers_arc: Arc<[Stealer<Arc<Task>>]> = Arc::from(_stealers.as_slice());
        let (tx, rx) = channel::unbounded();
        let (ttx, trx) = channel::unbounded();
        let spawner = Spawner::new(tx);
        let wait_group = WaitGroup::new();
        for _idx in 0..size {
            let worker = Worker::new_fifo();
            _stealers.push(worker.stealer());
            let ic = Arc::clone(&injector);
            let sc = Arc::clone(&stealers_arc);
            let wg = wait_group.clone();
            let rc = trx.clone();
            std::thread::spawn(move || {
                let (task_tx, task_rx) = channel::unbounded();
                let runner = TaskRunner {
                    _idx,
                    worker,
                    injector: ic,
                    stealers: sc,
                    wait_group: Some(wg),
                    rx: rc,
                    task_tx,
                    task_rx,
                };
                runner.run();
                drop(runner);
            });
        }
        let scheduler = Self {
            _size: size,
            injector,
            _stealers,
            wait_group,
            notifier: ttx,
            rx,
        };
        (spawner, scheduler)
    }
}

impl Scheduler for WorkStealingScheduler {
    fn init(size: usize) -> (Spawner, Self) {
        Self::new(size)
    }
    fn schedule(&mut self, future: BoxedFuture) {
        let task = Arc::new(Task {
            future,
            tx: Mutex::new(None),
        });
        self.injector.push(task);
        self.notifier
            .send(Message::HaveTasks)
            .expect("Failed to send message");
    }
    fn reschedule(&mut self, task: Arc<Task>) {
        self.injector.push(task);
        self.notifier
            .send(Message::HaveTasks)
            .expect("Failed to send message");
    }
    fn shutdown(self) {
        self.notifier
            .send(Message::Close)
            .expect("Failed to send message");
        self.wait_group.wait();
    }
    fn receiver(&self) -> &Receiver<ScheduleMessage> {
        &self.rx
    }
}

impl TaskRunner {
    fn run(&self) {
        let mut select = Select::new();
        let task_index = select.recv(&self.task_rx);
        let rx_index = select.recv(&self.rx);
        // let _scope = super::enter::enter().unwrap();
        'outer: loop {
            match self.worker.pop() {
                Some(task) => {
                    task.replace_tx(self.task_tx.clone());
                    task.run();
                }
                None => {
                    let mut wakeup_count = 0;
                    let mut steal_count = 0;
                    // First push in all the woke up Task, non-blocking.
                    loop {
                        match self.task_rx.try_recv() {
                            Ok(task) => {
                                wakeup_count += 1;
                                self.worker.push(task);
                            }
                            Err(channel::TryRecvError::Empty) => break,
                            Err(channel::TryRecvError::Disconnected) => break 'outer,
                        }
                    }
                    if wakeup_count > 0 {
                        continue;
                    }
                    // If we are starving, start stealing.
                    while let Some(task) = self.steal_task() {
                        steal_count += 1;
                        self.worker.push(task);
                    }
                    if steal_count > 0 {
                        continue;
                    }
                    // Finally, wait for a single wakeup task or broadcast signal from scheduler
                    let oprv = select.select();
                    match oprv.index() {
                        i if i == task_index => match oprv.recv(&self.task_rx) {
                            Ok(task) => self.worker.push(task),
                            Err(_) => break,
                        },
                        i if i == rx_index => match oprv.recv(&self.rx) {
                            Ok(Message::HaveTasks) => continue,
                            Ok(Message::Close) | Err(_) => break,
                        },
                        _ => unreachable!(),
                    }
                }
            }
        }
    }

    fn steal_task(&self) -> Option<Arc<Task>> {
        // will generate *ONE* task at a time
        std::iter::repeat_with(|| {
            self.injector
                .steal()
                .or_else(|| self.stealers.iter().map(|s| s.steal()).collect())
        })
        .find(|s| !s.is_retry())
        .and_then(|s| s.success())
    }
}

impl Drop for TaskRunner {
    fn drop(&mut self) {
        if let Some(w) = self.wait_group.take() {
            drop(w);
        }
    }
}
