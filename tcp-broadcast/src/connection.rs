//! Per-peer worker task: drains a queue of [`crate::Job`]s, opens one
//! TCP connection to `address`, writes each non-cancelled job's payload
//! length-prefixed (u32 BE) and signals delivery via the job's
//! `delivered` oneshot.
//!
//! On socket error: reconnects with exponential backoff (50ms..5s) and
//! continues with the NEXT job — the failing job is silently dropped
//! (its delivered oneshot never fires).  Use `tcp-reliable-sender` if
//! you need retries on the same payload.
//!
//! Cancelled jobs are popped off the queue and discarded without
//! touching the socket.  This is what bounds memory growth under
//! sustained crashes: the worker drains the queue at line rate even
//! when every job is cancelled.

use std::{net::SocketAddr, sync::atomic::Ordering, time::Duration};

use bytes::{BufMut, BytesMut};
use common::Options;
use socket2::SockRef;
use tokio::{io::AsyncWriteExt, net::TcpStream, time::sleep};

use crate::Job;

#[cfg(feature = "unbounded")]
type ChannelReceiver = tokio::sync::mpsc::UnboundedReceiver<Job>;
#[cfg(not(feature = "unbounded"))]
type ChannelReceiver = tokio::sync::mpsc::Receiver<Job>;

pub(crate) struct Connection {
    address: SocketAddr,
    receiver: ChannelReceiver,
    options: Options,
}

impl Connection {
    pub(crate) fn spawn(
        address: SocketAddr,
        receiver: ChannelReceiver,
        options: Options,
    ) {
        tokio::spawn(async move {
            Self {
                address,
                receiver,
                options,
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut backoff = Duration::from_millis(50);
        let max_backoff = Duration::from_secs(5);

        loop {
            // Connect (with exponential backoff on failure).
            let stream = match TcpStream::connect(self.address).await {
                Ok(s) => {
                    backoff = Duration::from_millis(50);
                    s
                }
                Err(e) => {
                    log::warn!(
                        "tcp-broadcast: connect {} failed: {} — retry in {:?}",
                        self.address,
                        e,
                        backoff
                    );
                    sleep(backoff).await;
                    backoff = (backoff * 2).min(max_backoff);
                    continue;
                }
            };

            // Apply socket tuning.
            if self.options.tcp_nodelay {
                let _ = stream.set_nodelay(true);
            }
            let sock_ref = SockRef::from(&stream);
            if let Some(size) = self.options.tcp_send_buffer {
                let _ = sock_ref.set_send_buffer_size(size);
            }
            if let Some(size) = self.options.tcp_recv_buffer {
                let _ = sock_ref.set_recv_buffer_size(size);
            }

            // Run the message loop.  Returns `Reconnect` on write error,
            // `Done` on channel close.
            match self.message_loop(stream).await {
                LoopOutcome::Reconnect => continue,
                LoopOutcome::Done => return,
            }
        }
    }

    async fn message_loop(&mut self, mut stream: TcpStream) -> LoopOutcome {
        // Per-message buffer reused across iterations to avoid realloc churn.
        let mut framed = BytesMut::with_capacity(self.options.write_buffer_size);

        while let Some(job) = self.receiver.recv().await {
            // Skip cancelled jobs entirely — don't touch the socket,
            // don't signal delivered.  This is the memory-bound guard:
            // a cancelled job is popped and dropped at line rate.
            if job.cancel.load(Ordering::Relaxed) {
                continue;
            }

            // Length-prefix: u32 BE matches LengthDelimitedCodec defaults
            // on the receiver side.
            framed.clear();
            framed.put_u32(job.payload.len() as u32);
            framed.extend_from_slice(&job.payload);

            if let Err(e) = stream.write_all(&framed).await {
                log::warn!(
                    "tcp-broadcast: write to {} failed: {} — reconnecting",
                    self.address,
                    e
                );
                // Drop this job (no delivered signal); reconnect.
                return LoopOutcome::Reconnect;
            }
            // Best-effort signal; if the receiver was dropped (caller
            // already cancelled or stopped caring) we don't care.
            let _ = job.delivered.send(());
        }

        // Channel closed (all senders dropped) — terminate worker.
        LoopOutcome::Done
    }
}

enum LoopOutcome {
    Reconnect,
    Done,
}
