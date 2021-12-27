use std::{
    collections::HashMap,
    io,
    ops::{Deref, DerefMut},
    time::Duration,
};

use futures_task::Waker;
use mio::{event::Source, Events, Interest, Poll, Registry, Token};
use once_cell::sync::Lazy;
use parking_lot::Mutex;

static REGISTRY: Lazy<Mutex<Option<Registry>>> = Lazy::new(|| Mutex::new(None));
static WAKER_MAP: Lazy<Mutex<HashMap<Token, WakerSet>>> = Lazy::new(|| Mutex::new(HashMap::new()));

pub struct Reactor {
    poll: Poll,
    events: Events,
}

pub struct WakerSet {
    read: Vec<Waker>,
    write: Vec<Waker>,
}

impl WakerSet {
    fn new() -> Self {
        Self {
            read: Vec::with_capacity(1024),
            write: Vec::with_capacity(1024),
        }
    }
}

impl Reactor {
    pub fn new() -> Self {
        let poll = mio::Poll::new().unwrap();
        let events = Events::with_capacity(1024);
        Self { poll, events }
    }

    pub fn wait(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.poll.poll(&mut self.events, timeout)?;
        let mut guard = WAKER_MAP.lock();
        let wakers_ref = guard.deref_mut();
        for e in self.events.iter() {
            if let Some(waker_set) = wakers_ref.get_mut(&e.token()) {
                if e.is_readable() && !waker_set.read.is_empty() {
                    waker_set.read.drain(..).for_each(|w| w.wake_by_ref());
                }
                if e.is_writable() && !waker_set.write.is_empty() {
                    waker_set.write.drain(..).for_each(|w| w.wake_by_ref());
                }
            }
        }
        Ok(())
    }
    pub fn setup_registry(&self) {
        let mut guard = REGISTRY.lock();
        let registry = self
            .poll
            .registry()
            .try_clone()
            .expect("Failed to clone registry");
        guard.replace(registry);
    }
}

pub(crate) fn add_waker(token: Token, interests: Interest, waker: Waker) -> io::Result<()> {
    let mut guard = WAKER_MAP.lock();
    let lock = guard.deref_mut();
    if let Some(ws) = lock.get_mut(&token) {
        if interests.is_readable() {
            ws.read.push(waker);
        } else if interests.is_writable() {
            ws.write.push(waker);
        }
    } else {
        let mut ws = WakerSet::new();
        if interests.is_readable() {
            ws.read.push(waker);
        } else if interests.is_writable() {
            ws.write.push(waker);
        }
        lock.insert(token, ws);
    }
    Ok(())
}

pub fn register<S>(source: &mut S, token: Token, interests: Interest) -> io::Result<()>
where
    S: Source + ?Sized,
{
    let registry_guard = REGISTRY.lock();
    if let Some(registry) = registry_guard.deref() {
        let mut wakers_guard = WAKER_MAP.lock();
        let wakers_ref = wakers_guard.deref_mut();
        if let Some(_) = wakers_ref.get(&token) {
            registry.reregister(source, token, interests)?;
        } else {
            registry.register(source, token, interests)?;
        }
    }
    Ok(())
}

pub fn deregister<S>(source: &mut S, token: Token) -> io::Result<()>
where
    S: Source + ?Sized,
{
    let registry_guard = REGISTRY.lock();
    if let Some(registry) = registry_guard.deref() {
        let mut wakers_guard = WAKER_MAP.lock();
        let wakers_ref = wakers_guard.deref_mut();
        if let Some(mut ws) = wakers_ref.remove(&token) {
            if !ws.read.is_empty() {
                ws.read.drain(..).for_each(|w| w.wake_by_ref());
            }
            if !ws.write.is_empty() {
                ws.write.drain(..).for_each(|w| w.wake_by_ref());
            }
        }
        registry.deregister(source)?;
    }
    Ok(())
}

// pub(crate) fn wait(timeout: Option<Duration>) -> io::Result<()> {
//     let mut guard = REACTOR.lock();
//     guard.deref_mut().wait(timeout)
// }
