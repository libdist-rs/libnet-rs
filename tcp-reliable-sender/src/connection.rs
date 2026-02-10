use std::{cmp::min, collections::VecDeque, future::poll_fn, net::SocketAddr, pin::Pin, task::Poll, time::Duration};

use bytes::Bytes;
use common::Options;
use futures::{Sink, Stream};
use socket2::SockRef;
use tokio::{net::TcpStream, sync::{mpsc::UnboundedReceiver, oneshot}, time::sleep};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

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

    async fn keep_alive(&mut self, stream: TcpStream) -> ConnectionError {
        log::debug!("Starting keep_alive for {}", self.address);
        // This buffer keeps all messages and handlers that we have successfully transmitted but for
        // which we are still waiting to receive an ACK.
        let mut pending_replies: VecDeque<_> = VecDeque::with_capacity(self.options.buffer_capacity);

        let (rd, wr) = stream.into_split();

        let mut read_codec = LengthDelimitedCodec::new();
        read_codec.set_max_frame_length(self.options.max_frame_length);
        let mut reader = FramedRead::new(rd, read_codec);

        let mut write_codec = LengthDelimitedCodec::new();
        write_codec.set_max_frame_length(self.options.max_frame_length);
        let mut writer = FramedWrite::new(wr, write_codec);
        writer.set_backpressure_boundary(self.options.write_buffer_size);

        let address = self.address;
        let buffer = &mut self.buffer;
        let receiver = &mut self.receiver;

        let error = poll_fn(|cx| {
            let mut progress = true;
            while progress {
                progress = false;

                // Phase 1: Flush pending writes
                match Pin::new(&mut writer).poll_flush(cx) {
                    Poll::Ready(Ok(())) => {}
                    Poll::Ready(Err(e)) => {
                        return Poll::Ready(ConnectionError::SendingFailed(address, e));
                    }
                    Poll::Pending => {}
                }

                // Phase 2: Drain send buffer (messages awaiting transmission)
                loop {
                    let front = buffer.front();
                    if front.is_none() {
                        break;
                    }
                    let (_, cancel_handler) = front.unwrap();
                    if cancel_handler.is_closed() {
                        buffer.pop_front();
                        continue;
                    }
                    match Pin::new(&mut writer).poll_ready(cx) {
                        Poll::Ready(Ok(())) => {
                            let (data, handler) = buffer.pop_front().unwrap();
                            if let Err(e) = Pin::new(&mut writer).start_send(data.clone()) {
                                // Put back and report error
                                buffer.push_front((data, handler));
                                return Poll::Ready(ConnectionError::SendingFailed(address, e));
                            }
                            log::debug!("Message sent to {}", address);
                            pending_replies.push_back((data, handler));
                            progress = true;
                        }
                        Poll::Pending => break,
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(ConnectionError::SendingFailed(address, e));
                        }
                    }
                }

                // Phase 3: Receive new messages from channel
                match receiver.poll_recv(cx) {
                    Poll::Ready(Some(InnerMsg { payload, cancel_handler })) => {
                        log::debug!("Received new message for {}", address);
                        buffer.push_back((payload, cancel_handler));
                        progress = true;
                    }
                    Poll::Ready(None) => {
                        log::warn!("Receiver channel closed for {}, flushing remaining data", address);
                        // Channel closed — flush remaining data then terminate
                        return match Pin::new(&mut writer).poll_close(cx) {
                            Poll::Ready(_) => Poll::Ready(ConnectionError::ChannelClosed(address)),
                            Poll::Pending => Poll::Pending,
                        };
                    }
                    Poll::Pending => {}
                }

                // Phase 4: Read ACK responses
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
