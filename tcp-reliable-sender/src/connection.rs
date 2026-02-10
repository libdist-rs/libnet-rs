use std::{cmp::min, collections::VecDeque, net::SocketAddr, time::Duration};

use bytes::Bytes;
use common::Options;
use futures::{SinkExt, StreamExt};
use socket2::SockRef;
use tokio::{net::TcpStream, sync::{mpsc::UnboundedReceiver, oneshot}, time::sleep};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

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

        let mut codec = LengthDelimitedCodec::new();
        codec.set_max_frame_length(self.options.max_frame_length);
        let mut framed_stream = Framed::new(stream, codec);

        let error = 'connection: loop {

            // Try to send all messages of the buffer.
            while let Some((data, cancel_handler)) = self.buffer.pop_front() {
                // Skip messages that have been cancelled.
                if cancel_handler.is_closed() {
                    continue;
                }

                // Try to send the message.
                match framed_stream.send(data.clone()).await {
                    Ok(()) => {
                        // The message has been sent, we remove it from the buffer and add it to
                        // `pending_replies` while we wait for an ACK.
                        log::debug!("Message sent to {}", self.address);
                        pending_replies.push_back((data, cancel_handler));
                    }
                    Err(e) => {
                        // We failed to send the message, we put it back into the buffer.
                        self.buffer.push_front((data, cancel_handler));
                        log::error!("Failed to send message to {}: {:?}", self.address, e);
                        break 'connection ConnectionError::SendingFailed(self.address, e);
                    }
                }
            }

            // Check if there are any new messages to send or if we get an ACK for messages we already sent.
            tokio::select! {
                msg_opt = self.receiver.recv() => {
                    if msg_opt.is_none() {
                        log::warn!("Receiver channel closed for {}", self.address);
                        break 'connection ConnectionError::ChannelClosed(self.address);
                    }
                    let InnerMsg { payload, cancel_handler } = msg_opt.unwrap();
                    log::debug!("Received new message for {}", self.address);
                    // Add the message to the buffer of messages to send.
                    self.buffer.push_back((payload, cancel_handler));
                }
                response_opt = framed_stream.next() => {
                    if response_opt.is_none() {
                        // Something has gone wrong (either the channel dropped or we failed to read from it).
                        // Put the message back in the buffer, we will try to send it again.
                        log::warn!("Connection closed for {}", self.address);
                        break 'connection ConnectionError::ConnectionClosed(self.address);
                    }
                    let response_opt = response_opt.unwrap();
                    if let Err(e) = response_opt {
                        log::warn!("Reading failed for {}: {:?}", self.address, e);
                        break 'connection ConnectionError::ReadingFailed(self.address, e);
                    }
                    let response = response_opt.unwrap();
                    let (_, handler) = match pending_replies.pop_front() {
                        Some(message) => message,
                        None => {
                            log::warn!("Unexpected ACK for {}", self.address);
                            break 'connection ConnectionError::UnexpectedAck(self.address);
                        },
                    };
                    let _ = handler.send(Ok(response.freeze()));
                }
            }
        };

        // If we reach this code, it means something went wrong. Put the messages for which we didn't receive an ACK
        // back into the sending buffer, we will try to send them again once we manage to establish a new connection.
        while let Some(message) = pending_replies.pop_front() {
            self.buffer.push_front(message);
        }

        log::debug!("Finished keep_alive for {}", self.address);
        error
    }
}
