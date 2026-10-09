use std::{
    fs::File,
    io::Read,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_core::Stream;

use crate::{Error, managed_copy::ManagedCopy};

pub struct ReadStream {
    file: Arc<Mutex<Option<File>>>,
    owner: Arc<ManagedCopy>,
    pub(crate) admitted: bool,
    ended: bool,
    chunk_size: usize,
}

impl ReadStream {
    pub(crate) fn new(
        file: File,
        chunk_size: usize,
        owner: Arc<ManagedCopy>,
    ) -> Result<Self, Error> {
        if chunk_size == 0 {
            return Err(Error::InvalidChunkSize);
        }
        let file = Arc::new(Mutex::new(Some(file)));
        owner.register_stream(&file);
        Ok(Self {
            file,
            chunk_size,
            owner,
            admitted: false,
            ended: false,
        })
    }
}

impl Stream for ReadStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }
        let file = Arc::clone(&self.file);
        let mut file = file.lock().expect("stream file lock");
        if !self.admitted
            && let Err(error) = self.owner.check()
        {
            file.take();
            self.ended = true;
            return Poll::Ready(Some(Err(error)));
        }
        let Some(file) = file.as_mut() else {
            self.ended = true;
            return Poll::Ready(None);
        };
        let mut chunk = vec![0; self.chunk_size];
        match file.read(&mut chunk) {
            Ok(0) => {
                self.ended = true;
                Poll::Ready(None)
            }
            Ok(len) => {
                chunk.truncate(len);
                Poll::Ready(Some(Ok(Bytes::from(chunk))))
            }
            Err(error) => Poll::Ready(Some(Err(error.into()))),
        }
    }
}

impl core::fmt::Debug for ReadStream {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter
            .debug_struct("ReadStream")
            .field("chunk_size", &self.chunk_size)
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}
