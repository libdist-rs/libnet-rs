use std::{cmp::min, collections::VecDeque, future::poll_fn, net::SocketAddr, pin::Pin, task::Poll, time::Duration};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use common::Options;
use futures::Stream;
use socket2::SockRef;
use tokio::{io::AsyncWrite, net::TcpStream, sync::{mpsc::Receiver, oneshot}, time::sleep};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

use crate::{ConnectionError, InnerMsg, SendError};

pub(crate) struct Connection
{
    /// The destination address.
    pub(super) address: SocketAddr,
    /// Channel from which the connection receives its commands.
    pub(super) receiver: Receiver<InnerMsg>,
    /// Configuration options.
    pub(super) options: Options,
    /// Buffer keeping all messages that need to be re-transmitted.
    pub(super) buffer: VecDeque<(Bytes, oneshot::Sender<Result<Bytes, SendError>>)>,
}

struct Waiter {
    delay: Duration,
    current: Duration,
    max_delay: Duration,
    retry: usize,
}

impl Waiter {
    fn new(delay: Duration, max_delay: Duration) -> Self {
        Self { delay, current: delay, max_delay, retry: 0 }
    }

    fn reset(&mut self) {
        self.current = self.delay;
        self.retry = 0;
    }

    fn attempt(&self) -> usize {
        self.retry
    }

    fn new_attempt(&mut self) {
        self.retry += 1;
        self.current = min(2*self.current, self.max_delay);
    }
}

impl Connection
{
    pub fn spawn(address: SocketAddr, receiver: Receiver<InnerMsg>, options: Options)
    {
        log::debug!("Connection spawning: {}", address);
        let buffer_capacity = options.buffer_capacity;
        let mut connection = Self {
            address,
            receiver,
            options,
            buffer: VecDeque::with_capacity(buffer_capacity),
        };

        log::debug!("Spawning connection task for {}", address);
        tokio::spawn(async move {
            log::debug!("Starting connection job: {}", address);
            std::panic::set_hook(Box::new(|panic_info| {
                log::error!("Panic occurred: {:?}", panic_info);
            }));
            if let Err(e) = connection.run().await {
                log::error!("Connection task for {} terminated with error: {:?}", address, e);
            }
        });
        log::debug!("Spawned connection task for {}", address);
    }

    pub(super) async fn run(&mut self) -> Result<(), ConnectionError>
    {
        log::debug!("Running Connection Loop for {}", self.address);
        let mut waiter = Waiter::new(self.options.retry_initial_delay, self.options.retry_max_delay);
        loop {
            match TcpStream::connect(self.address).await {
                Ok(stream) => {
                    log::info!("Connected to {}", self.address);
                    if self.options.tcp_nodelay {
                        if let Err(e) = stream.set_nodelay(true) {
                            log::warn!("Failed to set TCP_NODELAY for {}: {}", self.address, e);
                        }
                    }
                    // Apply socket buffer tuning
                    let sock_ref = SockRef::from(&stream);
                    if let Some(size) = self.options.tcp_send_buffer {
                        let _ = sock_ref.set_send_buffer_size(size);
                    }
                    if let Some(size) = self.options.tcp_recv_buffer {
                        let _ = sock_ref.set_recv_buffer_size(size);
                    }
                    // Reset the delay back to max
                    waiter.reset();

                    let error = self.keep_alive(stream).await;
                    log::warn!("Keep alive error: {:?}", error);
                },
                Err(e) => {
                    log::warn!(
                        "Failed to connect {} (Attempt: {}) with error {}", self.address,
                        waiter.attempt(),
                        e
                    );
                    let timer = sleep(waiter.current);
                    tokio::pin!(timer);

                    // Wait an increasing delay before attempting to reconnect.
                    timer.await;
                    waiter.new_attempt();
                }
            }
        }
    }

    async fn keep_alive(&mut self, mut stream: TcpStream) -> ConnectionError {
        log::debug!("Starting keep_alive for {}", self.address);
        // This buffer keeps all messages and handlers that we have successfully transmitted but for
        // which we are still waiting to receive an ACK.
        let mut pending_replies: VecDeque<(Bytes, oneshot::Sender<Result<Bytes, SendError>>)> =
            VecDeque::with_capacity(self.options.buffer_capacity);

        // Borrow-based split: zero overhead, no Arc, no Mutex
        let (rd, mut wr) = stream.split();

        let mut read_codec = LengthDelimitedCodec::new();
        read_codec.set_max_frame_length(self.options.max_frame_length);
        let mut reader = FramedRead::new(rd, read_codec);

        let address = self.address;
        let buffer = &mut self.buffer;
        let receiver = &mut self.receiver;
        let backpressure_boundary = self.options.write_buffer_size;
        let batch_drain_cap = self.options.batch_drain_cap;

        // Contiguous write buffer: frames encoded directly here, flushed via poll_write
        let mut write_buf = BytesMut::with_capacity(backpressure_boundary);

        let error = poll_fn(|cx| {
            let mut progress = true;
            while progress {
                progress = false;

                // Phase 1: Flush write buffer via poll_write
                while !write_buf.is_empty() {
                    match Pin::new(&mut wr).poll_write(cx, &write_buf) {
                        Poll::Ready(Ok(0)) => {
                            return Poll::Ready(ConnectionError::SendingFailed(
                                address,
                                std::io::Error::new(std::io::ErrorKind::WriteZero, "write returned 0"),
                            ));
                        }
                        Poll::Ready(Ok(n)) => {
                            progress = true;
                            write_buf.advance(n);
                        }
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(ConnectionError::SendingFailed(address, e));
                        }
                        Poll::Pending => break,
                    }
                }

                // Reclaim capacity when fully drained
                if write_buf.is_empty() {
                    write_buf.clear();
                }

                // Phase 2: Flush kernel buffer when write buffer is empty
                if write_buf.is_empty() {
                    match Pin::new(&mut wr).poll_flush(cx) {
                        Poll::Ready(Ok(())) => {}
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(ConnectionError::SendingFailed(address, e));
                        }
                        Poll::Pending => {}
                    }
                }

                // Phase 3: Drain send buffer into write buffer (respecting backpressure)
                if write_buf.len() < backpressure_boundary {
                    let mut drained = 0;
                    loop {
                        if drained >= batch_drain_cap || write_buf.len() >= backpressure_boundary {
                            break;
                        }
                        let front = buffer.front();
                        if front.is_none() {
                            break;
                        }
                        let (_, cancel_handler) = front.unwrap();
                        if cancel_handler.is_closed() {
                            buffer.pop_front();
                            continue;
                        }
                        let (data, handler) = buffer.pop_front().unwrap();
                        write_buf.put_u32(data.len() as u32);
                        write_buf.extend_from_slice(&data);
                        log::debug!("Message sent to {}", address);
                        pending_replies.push_back((data, handler));
                        drained += 1;
                        progress = true;
                    }
                }

                // Phase 4: Receive new messages from channel
                match receiver.poll_recv(cx) {
                    Poll::Ready(Some(InnerMsg { payload, cancel_handler })) => {
                        log::debug!("Received new message for {}", address);
                        buffer.push_back((payload, cancel_handler));
                        progress = true;
                    }
                    Poll::Ready(None) => {
                        log::warn!("Receiver channel closed for {}, flushing remaining data", address);
                        if !write_buf.is_empty() {
                            progress = true;
                            continue;
                        }
                        return match Pin::new(&mut wr).poll_shutdown(cx) {
                            Poll::Ready(_) => Poll::Ready(ConnectionError::ChannelClosed(address)),
                            Poll::Pending => Poll::Pending,
                        };
                    }
                    Poll::Pending => {}
                }

                // Phase 5: Read ACK responses
                match Pin::new(&mut reader).poll_next(cx) {
                    Poll::Ready(Some(Ok(response))) => {
                        progress = true;
                        match pending_replies.pop_front() {
                            Some((_, handler)) => {
                                let _ = handler.send(Ok(response.freeze()));
                            }
                            None => {
                                log::warn!("Unexpected ACK for {}", address);
                                return Poll::Ready(ConnectionError::UnexpectedAck(address));
                            }
                        }
                    }
                    Poll::Ready(Some(Err(e))) => {
                        log::warn!("Reading failed for {}: {:?}", address, e);
                        return Poll::Ready(ConnectionError::ReadingFailed(address, e));
                    }
                    Poll::Ready(None) => {
                        log::warn!("Connection closed for {}", address);
                        return Poll::Ready(ConnectionError::ConnectionClosed(address));
                    }
                    Poll::Pending => {}
                }
            }
            Poll::Pending
        }).await;

        // If we reach this code, it means something went wrong. Put the messages for which we didn't receive an ACK
        // back into the sending buffer, we will try to send them again once we manage to establish a new connection.
        while let Some(message) = pending_replies.pop_front() {
            self.buffer.push_front(message);
        }

        log::debug!("Finished keep_alive for {}", self.address);
        error
    }
}
