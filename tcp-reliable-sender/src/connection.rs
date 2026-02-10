use std::{cmp::min, collections::VecDeque, future::poll_fn, io::IoSlice, net::SocketAddr, pin::Pin, task::Poll, time::Duration};

use bytes::{BufMut, Bytes, BytesMut};
use common::Options;
use futures::Stream;
use socket2::SockRef;
use tokio::{io::AsyncWrite, net::TcpStream, sync::{mpsc::UnboundedReceiver, oneshot}, time::sleep};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

use crate::{ConnectionError, InnerMsg, SendError};

pub(crate) struct Connection
{
    /// The destination address.
    pub(super) address: SocketAddr,
    /// Channel from which the connection receives its commands.
    pub(super) receiver: UnboundedReceiver<InnerMsg>,
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

/// Encode a message as a length-delimited frame: [4-byte BE length][payload].
#[inline]
fn encode_frame(payload: &Bytes) -> Bytes {
    let len = payload.len() as u32;
    let mut buf = BytesMut::with_capacity(4 + payload.len());
    buf.put_u32(len);
    buf.extend_from_slice(payload);
    buf.freeze()
}

/// Maximum number of IoSlices to pass to a single writev call.
const MAX_IOVECS: usize = 64;

impl Connection
{
    pub fn spawn(address: SocketAddr, receiver: UnboundedReceiver<InnerMsg>, options: Options)
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

        // Write queue: encoded frames waiting to be written via writev.
        // Each entry corresponds 1:1 with a message moved to pending_replies.
        let mut write_queue: VecDeque<Bytes> = VecDeque::with_capacity(128);
        let mut front_offset: usize = 0;
        let mut buffered: usize = 0;

        let error = poll_fn(|cx| {
            let mut progress = true;
            while progress {
                progress = false;

                // Phase 1: Flush write queue via writev
                while !write_queue.is_empty() {
                    let mut slices_buf: [IoSlice<'_>; MAX_IOVECS] = std::array::from_fn(|_| IoSlice::new(&[]));
                    let mut n_slices = 0;

                    for (i, frame) in write_queue.iter().enumerate() {
                        if i >= MAX_IOVECS { break; }
                        let slice = if i == 0 {
                            &frame[front_offset..]
                        } else {
                            &frame[..]
                        };
                        if slice.is_empty() { continue; }
                        slices_buf[n_slices] = IoSlice::new(slice);
                        n_slices += 1;
                    }

                    if n_slices == 0 { break; }

                    match Pin::new(&mut wr).poll_write_vectored(cx, &slices_buf[..n_slices]) {
                        Poll::Ready(Ok(0)) => {
                            return Poll::Ready(ConnectionError::SendingFailed(
                                address,
                                std::io::Error::new(std::io::ErrorKind::WriteZero, "write returned 0"),
                            ));
                        }
                        Poll::Ready(Ok(n)) => {
                            progress = true;
                            buffered -= n;

                            let mut remaining = n;
                            while remaining > 0 {
                                let front = &write_queue[0];
                                let avail = front.len() - front_offset;
                                if remaining >= avail {
                                    remaining -= avail;
                                    write_queue.pop_front();
                                    front_offset = 0;
                                } else {
                                    front_offset += remaining;
                                    remaining = 0;
                                }
                            }
                        }
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(ConnectionError::SendingFailed(address, e));
                        }
                        Poll::Pending => break,
                    }
                }

                // Phase 2: Flush kernel buffer when write queue is empty
                if write_queue.is_empty() && buffered == 0 {
                    match Pin::new(&mut wr).poll_flush(cx) {
                        Poll::Ready(Ok(())) => {}
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(ConnectionError::SendingFailed(address, e));
                        }
                        Poll::Pending => {}
                    }
                }

                // Phase 3: Drain send buffer (messages awaiting transmission) into write queue
                if buffered < backpressure_boundary {
                    let mut drained = 0;
                    loop {
                        if drained >= batch_drain_cap || buffered >= backpressure_boundary {
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
                        let frame = encode_frame(&data);
                        buffered += frame.len();
                        write_queue.push_back(frame);
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
                        // Channel closed — flush remaining data then terminate
                        if !write_queue.is_empty() {
                            progress = true;
                            // Continue loop to flush write queue first
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
