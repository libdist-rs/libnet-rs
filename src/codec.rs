use std::io;
use tokio_util::codec::{LengthDelimitedCodec, Decoder, Encoder};
use bytes::{Bytes, BytesMut};
use crate::Message;

/// Encoding and decoding messages on the network
pub struct Decodec<O> (pub LengthDelimitedCodec, std::marker::PhantomData<O>);
impl<O> Decodec<O> {
    pub fn new() -> Self {
        let length_codec = LengthDelimitedCodec::builder()
            .length_field_type::<u64>()
            .new_codec();
        Decodec(length_codec,std::marker::PhantomData::<O>)
    }
}

impl<O> Default for Decodec<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Msg> Decoder for Decodec<Msg> 
where 
    Msg: Message,
{
    type Item = Msg;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.0.decode(src)? {
            Some(in_data) => {Ok(
                Some(Msg::from_bytes(&in_data))
            )},
            None => Ok(None),
        }
    }
}

#[derive(Debug)]
pub struct EnCodec<I> (pub LengthDelimitedCodec, std::marker::PhantomData<I>);

impl<I> EnCodec<I> {
    pub fn new() -> Self {
        let length_codec = LengthDelimitedCodec::builder()
            .length_field_type::<u64>()
            .new_codec();

        EnCodec(length_codec,std::marker::PhantomData::<I>)
    }
}

impl<I> Default for EnCodec<I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I> std::clone::Clone for EnCodec<I> {
    fn clone(&self) -> Self {
        EnCodec::new()
    }
}

impl<I> Encoder<I> for EnCodec<I> 
where I:Message,
{
    type Error = io::Error;

    fn encode(&mut self, item: I, dst:&mut BytesMut) -> Result<(),Self::Error> {
        let data = I::to_bytes(&item);
        let buf = Bytes::from(data);
        self.0.encode(buf, dst)
    }
}