use std::io;

use futures_lite::prelude::*;
use lib::{multi_thread, schedulers, single_thread, AsyncTcpListener, AsyncTcpStream};

mod lib;

fn main() -> io::Result<()> {
    // let executor = single_thread::Executor::new(1024);
    let executor: multi_thread::Executor<schedulers::work_stealing::WorkStealingScheduler> =
        multi_thread::Executor::new();
    executor.block_on(server());
    Ok(())
}

async fn server() {
    let mut listener = AsyncTcpListener::new("0.0.0.0:9000").unwrap();
    let mut incoming = listener.incoming();
    while let Ok(next) = incoming.try_next().await {
        match next {
            Some(stream) => multi_thread::spawn(handler(stream)),
            // Some(stream) => single_thread::spawn(handler(stream)),
            None => {
                eprintln!("No incoming stream.");
                break;
            }
        }
    }
}

async fn handler(mut stream: AsyncTcpStream) {
    loop {
        let mut buf: Vec<u8> = vec![0u8; 4096];
        match stream.read(&mut buf).await {
            Ok(0) => break,
            Ok(_n) => {
                let input = String::from_utf8_lossy(&buf);
                let pat: &[_] = &['\0', '\r', '\n'];
                let msg = format!("[Server]: Got <{}>\n", &input.trim_end_matches(pat));
                stream.write_all(msg.as_bytes()).await.unwrap();
            }
            Err(_e) => break,
        }
    }
}
