use bytes::Bytes;
use common::Message;
pub use common::UdpOptions;
use futures::Stream;
use std::{
    marker::PhantomData,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::mpsc::UnboundedReceiver;

mod job;
use job::*;

mod error;
pub use error::*;

pub struct UdpReceiver<RecvMsg> {
    _x: PhantomData<RecvMsg>,
    rx: UnboundedReceiver<(SocketAddr, Bytes)>,
}

/// Safety: RecvMsg is a phantom type; only Bytes are used internally.
unsafe impl<RecvMsg> Send for UdpReceiver<RecvMsg> {}
unsafe impl<RecvMsg> Sync for UdpReceiver<RecvMsg> {}

impl<RecvMsg> Unpin for UdpReceiver<RecvMsg> {}

/// Yields `(SocketAddr, RecvMsg)` — includes the sender's address with each message.
impl<RecvMsg> Stream for UdpReceiver<RecvMsg>
where
    RecvMsg: Message,
{
    type Item = Result<(SocketAddr, RecvMsg), RecvMsg::DeserializationError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some((addr, msg))) => {
                let data = RecvMsg::from_bytes(&msg);
                log::debug!("Received message from {}: {:?}", addr, msg);
                Poll::Ready(Some(data.map(|m| (addr, m))))
            }
            Poll::Ready(None) => {
                log::warn!("UDP receiver channel closed");
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<RecvMsg> UdpReceiver<RecvMsg> {
    fn new(rx: UnboundedReceiver<(SocketAddr, Bytes)>) -> Self {
        Self {
            _x: PhantomData,
            rx,
        }
    }

    pub fn spawn(address: SocketAddr) -> Self {
        Self::spawn_with_options(address, UdpOptions::default())
    }

    pub fn spawn_with_options(address: SocketAddr, options: UdpOptions) -> Self {
        let rx = UdpReceiverJob::spawn(address, options);
        Self::new(rx)
    }

    /// Convert into a stream that yields only messages (without sender address).
    /// Useful for TCP API compatibility.
    pub fn messages_only(self) -> MessagesOnly<RecvMsg> {
        MessagesOnly { inner: self }
    }
}

/// Adapter that strips the sender `SocketAddr` from each received message.
/// Created via [`UdpReceiver::messages_only()`].
pub struct MessagesOnly<RecvMsg> {
    inner: UdpReceiver<RecvMsg>,
}

unsafe impl<RecvMsg> Send for MessagesOnly<RecvMsg> {}
unsafe impl<RecvMsg> Sync for MessagesOnly<RecvMsg> {}

impl<RecvMsg> Unpin for MessagesOnly<RecvMsg> {}

impl<RecvMsg> Stream for MessagesOnly<RecvMsg>
where
    RecvMsg: Message,
{
    type Item = Result<RecvMsg, RecvMsg::DeserializationError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok((_addr, msg)))) => Poll::Ready(Some(Ok(msg))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
