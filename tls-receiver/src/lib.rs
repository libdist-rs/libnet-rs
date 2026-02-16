use bytes::Bytes;
use common::Message;
pub use common::TlsOptions;
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

pub struct TlsReceiver<RecvMsg> {
    _x: PhantomData<RecvMsg>,
    rx_from_connections: UnboundedReceiver<Bytes>,
}

/// We only use RecvMsg as a phantom type, so it's safe to send and sync
unsafe impl<RecvMsg> Send for TlsReceiver<RecvMsg> {}
unsafe impl<RecvMsg> Sync for TlsReceiver<RecvMsg> {}

impl<RecvMsg> Unpin for TlsReceiver<RecvMsg> {}

impl<RecvMsg> Stream for TlsReceiver<RecvMsg>
where
    RecvMsg: Message,
{
    type Item = Result<RecvMsg, RecvMsg::DeserializationError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx_from_connections.poll_recv(cx) {
            Poll::Ready(Some(msg)) => {
                let data = RecvMsg::from_bytes(&msg);
                log::debug!("Received message: {:?}", msg);
                Poll::Ready(Some(data))
            }
            Poll::Ready(None) => {
                log::warn!("Connection closed");
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<RecvMsg> TlsReceiver<RecvMsg> {
    fn new(rx_from_connections: UnboundedReceiver<Bytes>) -> Self {
        Self {
            _x: PhantomData,
            rx_from_connections,
        }
    }

    pub fn spawn(address: SocketAddr) -> Self {
        Self::spawn_with_options(address, TlsOptions::default())
    }

    pub fn spawn_with_options(address: SocketAddr, options: TlsOptions) -> Self {
        let rx_net_msgs = TlsReceiverJob::spawn(address, options);
        Self::new(rx_net_msgs)
    }
}
