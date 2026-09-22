use std::{
    fs::File,
    io::Read,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_core::Stream;

use crate::Error;

#[derive(Debug)]
pub struct ReadStream {
    file: File,
    chunk_size: usize,
}

impl ReadStream {
    pub(crate) fn new(file: File, chunk_size: usize) -> Result<Self, Error> {
        if chunk_size == 0 {
            return Err(Error::InvalidChunkSize);
        }
        Ok(Self { file, chunk_size })
    }
}

impl Stream for ReadStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut chunk = vec![0; self.chunk_size];
        match self.file.read(&mut chunk) {
            Ok(0) => Poll::Ready(None),
            Ok(len) => {
                chunk.truncate(len);
                Poll::Ready(Some(Ok(Bytes::from(chunk))))
            }
            Err(error) => Poll::Ready(Some(Err(error.into()))),
        }
    }
}
