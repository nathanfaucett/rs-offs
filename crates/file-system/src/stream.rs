use std::sync::Arc;

use bytes::Bytes;
use futures_core::Stream;
use futures_util::stream::unfold;

use crate::{Error, content::ContentObject, managed_copy::ManagedCopy};

/// Asynchronous chunk reads of one Content Revision, driven by the content port.
pub struct ReadStream {
    inner: BoxStream,
}

#[cfg(not(target_arch = "wasm32"))]
type BoxStream = std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, Error>> + Send>>;

#[cfg(target_arch = "wasm32")]
type BoxStream = std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, Error>>>>;

struct Chunks {
    content: Arc<dyn ContentObject>,
    owner: Arc<ManagedCopy>,
    file_id: uuid::Uuid,
    revision: uuid::Uuid,
    offset: u64,
    chunk_size: usize,
    admitted: bool,
    ended: bool,
}

impl ReadStream {
    pub(crate) fn new(
        content: Arc<dyn ContentObject>,
        file_id: uuid::Uuid,
        revision: uuid::Uuid,
        chunk_size: usize,
        owner: Arc<ManagedCopy>,
        admitted: bool,
    ) -> Result<Self, Error> {
        if chunk_size == 0 {
            return Err(Error::InvalidChunkSize);
        }
        let state = Chunks {
            content,
            owner,
            file_id,
            revision,
            offset: 0,
            chunk_size,
            admitted,
            ended: false,
        };
        Ok(Self {
            inner: Box::pin(unfold(state, |mut state| async move {
                if state.ended {
                    return None;
                }
                if !state.admitted
                    && let Err(error) = state.owner.check()
                {
                    state.ended = true;
                    return Some((Err(error), state));
                }
                match state
                    .content
                    .read_range(
                        state.file_id,
                        state.revision,
                        state.offset,
                        u32::try_from(state.chunk_size).unwrap_or(u32::MAX),
                    )
                    .await
                {
                    Ok(chunk) if chunk.is_empty() => None,
                    Ok(chunk) => {
                        state.offset += chunk.len() as u64;
                        Some((Ok(chunk), state))
                    }
                    Err(error) => {
                        state.ended = true;
                        Some((Err(error), state))
                    }
                }
            })),
        })
    }
}

impl Stream for ReadStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(context)
    }
}

impl core::fmt::Debug for ReadStream {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.debug_struct("ReadStream").finish_non_exhaustive()
    }
}
