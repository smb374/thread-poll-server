mod enter;
pub mod multi_thread;
mod reactor;
pub mod schedulers;
pub mod single_thread;

use futures_lite::{AsyncRead, AsyncWrite, Stream};
use futures_task::Context;
use mio::{unix::SourceFd, Interest, Token};
use std::{
    io::{self, prelude::*},
    net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs},
    os::unix::prelude::*,
    pin::Pin,
    task::Poll,
};

pub struct AsyncTcpListener {
    listener: TcpListener,
    token: Token,
}

pub struct AsyncIncoming<'a> {
    inner: &'a mut AsyncTcpListener,
}

pub struct AsyncTcpStream {
    stream: TcpStream,
    token: Token,
}

impl AsyncTcpListener {
    pub fn new<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;
        let fd = listener.as_raw_fd();
        let token = Token(fd as usize);
        Ok(Self { listener, token })
    }
    pub fn incoming(&mut self) -> AsyncIncoming<'_> {
        AsyncIncoming { inner: self }
    }
}

impl Drop for AsyncTcpListener {
    fn drop(&mut self) {
        let fd = self.listener.as_raw_fd();
        reactor::deregister(&mut SourceFd(&fd), self.token).expect("Failed to deregister listener");
    }
}

impl<'a> Stream for AsyncIncoming<'a> {
    type Item = io::Result<AsyncTcpStream>;
    // type Item = AsyncTcpStream;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        match me.inner.listener.accept() {
            Ok((stream, _addr)) => Poll::Ready(Some(Ok(AsyncTcpStream::new(stream)))),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // register event
                let waker = cx.waker().clone();
                reactor::register(
                    &mut SourceFd(&me.inner.listener.as_raw_fd()),
                    me.inner.token,
                    Interest::READABLE,
                )
                .expect("Failed to register listener");
                reactor::add_waker(me.inner.token, Interest::READABLE, waker)
                    .expect("failed to add waker");
                Poll::Pending
            }
            Err(e) => {
                eprintln!("accept error: {}", e.to_string());
                Poll::Ready(None)
            }
        }
    }
}

impl AsyncTcpStream {
    fn new(stream: TcpStream) -> Self {
        stream
            .set_nonblocking(true)
            .expect("failed to set nonblocking");
        let token = Token(stream.as_raw_fd() as usize);
        Self { stream, token }
    }
}

impl AsyncRead for AsyncTcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let me = self.get_mut();
        match me.stream.read(buf) {
            Ok(i) => Poll::Ready(Ok(i)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let waker = cx.waker().clone();
                reactor::register(
                    &mut SourceFd(&me.stream.as_raw_fd()),
                    me.token,
                    Interest::READABLE,
                )
                .expect("Failed to register stream");
                reactor::add_waker(me.token, Interest::READABLE, waker)
                    .expect("failed to add waker");
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl AsyncWrite for AsyncTcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let me = self.get_mut();
        match me.stream.write(buf) {
            Ok(i) => Poll::Ready(Ok(i)),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let waker = cx.waker().clone();
                reactor::register(
                    &mut SourceFd(&me.stream.as_raw_fd()),
                    me.token,
                    Interest::WRITABLE,
                )
                .expect("Failed to register stream");
                reactor::add_waker(me.token, Interest::WRITABLE, waker)
                    .expect("failed to add waker");
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        match me.stream.flush() {
            Ok(()) => Poll::Ready(Ok(())),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let waker = cx.waker().clone();
                reactor::register(
                    &mut SourceFd(&me.stream.as_raw_fd()),
                    me.token,
                    Interest::WRITABLE,
                )
                .expect("Failed to register stream");
                reactor::add_waker(me.token, Interest::WRITABLE, waker)
                    .expect("failed to add waker");
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        match me.stream.shutdown(Shutdown::Both) {
            Ok(()) => Poll::Ready(Ok(())),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                let waker = cx.waker().clone();
                reactor::register(
                    &mut SourceFd(&me.stream.as_raw_fd()),
                    me.token,
                    Interest::READABLE.add(Interest::WRITABLE),
                )
                .expect("Failed to register stream");
                reactor::add_waker(me.token, Interest::READABLE.add(Interest::WRITABLE), waker)
                    .expect("failed to add waker");
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl Drop for AsyncTcpStream {
    fn drop(&mut self) {
        if let Err(e) = reactor::deregister(&mut SourceFd(&self.stream.as_raw_fd()), self.token) {
            eprintln!("Failed to deregister stream {}: {}", self.token.0, e);
        }
    }
}
