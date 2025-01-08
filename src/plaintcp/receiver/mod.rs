use bytes::Bytes;
use std::{
    future::Future,
    marker::PhantomData,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
};

use crate::Message;

mod job;
use job::*;

mod error;
pub use error::*;

pub struct TcpReceiver<RecvMsg> {
    _x: PhantomData<RecvMsg>,
    rx_from_connections: UnboundedReceiver<Bytes>,
}

/// We only use RecvMsg as a phantom type, so it's safe to send and sync
unsafe impl<RecvMsg> Send for TcpReceiver<RecvMsg> {}
unsafe impl<RecvMsg> Sync for TcpReceiver<RecvMsg> {}

impl<RecvMsg> Future for TcpReceiver<RecvMsg>
where
    RecvMsg: Message + Unpin,
{
    type Output = Option<RecvMsg>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
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

impl<RecvMsg> TcpReceiver<RecvMsg> {
    fn new(rx_from_connections: UnboundedReceiver<Bytes>) -> Self {
        Self {
            _x: PhantomData,
            rx_from_connections,
        }
    }

    pub fn spawn(address: SocketAddr) -> Self {
        let rx_net_msgs = TcpReceiverJob::spawn(address);
        Self::new(rx_net_msgs)
    }
}
