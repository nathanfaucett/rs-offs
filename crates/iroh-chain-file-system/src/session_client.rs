use super::{AccessTokenProvider, RootIrohTransport};
use bytes::Bytes;
use file_system::{
    FileHandle, FileHandleId, FileOperation, FileResponse, FileSessionService, OpenRequest,
    ScanPage,
};
use iroh::{
    EndpointId,
    endpoint::{RecvStream, SendStream},
};
use iroh_chain::{AllowedEndpointId, FILE_TRANSFER_ALPN, RootAuthorizer};
use std::io::{Error, ErrorKind};

pub struct RemoteFileHandle<A, V, P>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
    P: AccessTokenProvider,
{
    send: SendStream,
    recv: RecvStream,
    peer: EndpointId,
    handle: FileHandleId,
    revision: deckv::Timestamp,
    transport: RootIrohTransport<A, V, P>,
}

impl<A, V, P> FileSessionService<EndpointId> for RootIrohTransport<A, V, P>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
    P: AccessTokenProvider,
{
    type Handle = RemoteFileHandle<A, V, P>;

    async fn open(&self, peer: EndpointId, request: OpenRequest) -> Result<Self::Handle, Error> {
        let (mut send, mut recv) = self.open_session(peer).await?;
        self.send_operation(&mut send, peer, FileOperation::Open(request))
            .await?;
        loop {
            let response = next_response(&mut recv).await?;
            match response {
                FileResponse::Opened {
                    handle, revision, ..
                } => {
                    return Ok(RemoteFileHandle {
                        send,
                        recv,
                        peer,
                        handle,
                        revision,
                        transport: self.clone(),
                    });
                }
                FileResponse::Error(error) => {
                    return Err(Error::other(format!("file service error: {error:?}")));
                }
                _ => {}
            }
        }
    }

    fn subscribe_file_sessions(
        &self,
    ) -> tokio::sync::broadcast::Receiver<file_system::SessionRequest<EndpointId>> {
        self.inner.sessions.subscribe()
    }
}

impl<A, V, P> RootIrohTransport<A, V, P>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
    P: AccessTokenProvider,
{
    async fn open_session(&self, peer: EndpointId) -> Result<(SendStream, RecvStream), Error> {
        let address = self
            .inner
            .peers
            .lock()
            .expect("peer lock poisoned")
            .get(&peer)
            .cloned()
            .ok_or_else(|| Error::new(ErrorKind::NotConnected, "Iroh peer is not connected"))?;
        let connection = self
            .inner
            .manager
            .endpoint()
            .connect(address, FILE_TRANSFER_ALPN)
            .await
            .map_err(Error::other)?;
        connection.open_bi().await.map_err(Error::other)
    }

    async fn send_operation(
        &self,
        send: &mut SendStream,
        peer: EndpointId,
        operation: FileOperation,
    ) -> Result<(), Error> {
        let authorization = self
            .inner
            .authorization
            .access_token(self.inner.root_id, self.inner.manager.endpoint().id(), peer)
            .await?;
        crate::scoped_transport::write_frame(
            send,
            &crate::scoped_transport::FileSessionRequestFrame {
                root_id: *self.inner.root_id.as_bytes(),
                authorization,
                operation,
            },
        )
        .await
    }
}

async fn next_response(recv: &mut RecvStream) -> Result<FileResponse<EndpointId>, Error> {
    let frame = crate::scoped_transport::read_frame::<
        _,
        crate::scoped_transport::FileSessionResponseFrame,
    >(recv)
    .await?
    .ok_or_else(|| Error::new(ErrorKind::UnexpectedEof, "file session ended"))?;
    let crate::scoped_transport::FileSessionResponseFrame::Response(response) = frame;
    Ok(response)
}

impl<A, V, P> FileHandle<EndpointId> for RemoteFileHandle<A, V, P>
where
    A: AllowedEndpointId,
    V: RootAuthorizer,
    P: AccessTokenProvider,
{
    type Error = Error;

    fn read<'a>(
        &'a mut self,
        offset: u64,
        length: u32,
    ) -> file_system::FileFuture<'a, Result<Bytes, Error>> {
        Box::pin(async move {
            self.transport
                .send_operation(
                    &mut self.send,
                    self.peer,
                    FileOperation::Read {
                        handle: self.handle.clone(),
                        offset,
                        length,
                    },
                )
                .await?;
            let mut bytes = Vec::new();
            loop {
                match next_response(&mut self.recv).await? {
                    FileResponse::ReadChunk(chunk) => bytes.extend_from_slice(&chunk),
                    FileResponse::ReadEnd => return Ok(Bytes::from(bytes)),
                    FileResponse::Error(error) => {
                        return Err(Error::other(format!("file service error: {error:?}")));
                    }
                    _ => {}
                }
            }
        })
    }

    fn write<'a>(
        &'a mut self,
        offset: u64,
        data: Bytes,
    ) -> file_system::FileFuture<'a, Result<u32, Error>> {
        Box::pin(async move {
            self.transport
                .send_operation(
                    &mut self.send,
                    self.peer,
                    FileOperation::Write {
                        handle: self.handle.clone(),
                        expected_revision: self.revision,
                        offset,
                        data,
                    },
                )
                .await?;
            loop {
                match next_response(&mut self.recv).await? {
                    FileResponse::Written { count, revision } => {
                        self.revision = revision;
                        return Ok(count);
                    }
                    FileResponse::Error(error) => {
                        return Err(Error::other(format!("file service error: {error:?}")));
                    }
                    _ => {}
                }
            }
        })
    }

    fn scan<'a>(
        &'a mut self,
        cursor: Option<String>,
        limit: u32,
    ) -> file_system::FileFuture<'a, Result<ScanPage<EndpointId>, Error>> {
        Box::pin(async move {
            self.transport
                .send_operation(
                    &mut self.send,
                    self.peer,
                    FileOperation::Scan {
                        handle: self.handle.clone(),
                        cursor,
                        limit,
                    },
                )
                .await?;
            loop {
                match next_response(&mut self.recv).await? {
                    FileResponse::Page(page) => return Ok(page),
                    FileResponse::Error(error) => {
                        return Err(Error::other(format!("file service error: {error:?}")));
                    }
                    _ => {}
                }
            }
        })
    }

    fn close(mut self: Box<Self>) -> file_system::FileFuture<'static, Result<(), Error>> {
        Box::pin(async move {
            self.transport
                .send_operation(
                    &mut self.send,
                    self.peer,
                    FileOperation::Close {
                        handle: self.handle.clone(),
                    },
                )
                .await?;
            loop {
                match next_response(&mut self.recv).await? {
                    FileResponse::Closed => {
                        self.send.finish().map_err(Error::other)?;
                        return Ok(());
                    }
                    FileResponse::Error(error) => {
                        return Err(Error::other(format!("file service error: {error:?}")));
                    }
                    _ => {}
                }
            }
        })
    }
}
