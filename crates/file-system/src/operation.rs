use std::{future::Future, sync::Arc};

use tokio::sync::watch;
use uuid::Uuid;

use crate::{Error, FileFuture, FileOperation};

#[derive(Clone, Debug)]
pub enum RemoteOperation {
    ImportSnapshots,
    ExportSnapshots,
    Session {
        operation: FileOperation,
        path: Option<String>,
    },
    PublishContent {
        file_id: Uuid,
        revision: Uuid,
    },
}

type Decision<PeerId> = dyn Fn(PeerId, RemoteOperation) -> FileFuture<'static, bool> + Send + Sync;

/// Caller-local policy and one-way cancellation for one sync executor.
/// Admission is linearized by the final cancellation read after the decision completes.
/// Admitted mutations finish without cancellation selection; outbound delivery waits are cancellable.
pub struct OperationControl<PeerId> {
    decision: Arc<Decision<PeerId>>,
    cancelled: watch::Sender<bool>,
}

impl<PeerId> Clone for OperationControl<PeerId> {
    fn clone(&self) -> Self {
        Self {
            decision: Arc::clone(&self.decision),
            cancelled: self.cancelled.clone(),
        }
    }
}

impl<PeerId> OperationControl<PeerId> {
    pub fn new(
        decision: impl Fn(PeerId, RemoteOperation) -> FileFuture<'static, bool> + Send + Sync + 'static,
    ) -> Self {
        Self {
            decision: Arc::new(decision),
            cancelled: watch::channel(false).0,
        }
    }

    pub fn cancel(&self) {
        self.cancelled.send_replace(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.cancelled.borrow()
    }

    pub async fn cancelled(&self) {
        let mut receiver = self.cancelled.subscribe();
        let _ = receiver.wait_for(|cancelled| *cancelled).await;
    }

    pub(crate) async fn wait<F: Future>(&self, future: F) -> Result<F::Output, Error> {
        tokio::select! {
            biased;
            () = self.cancelled() => Err(Error::Cancelled),
            output = future => Ok(output),
        }
    }

    pub(crate) async fn admit(
        &self,
        peer: PeerId,
        operation: RemoteOperation,
    ) -> Result<(), Error> {
        let allowed = self.wait((self.decision)(peer, operation)).await?;
        if self.is_cancelled() {
            Err(Error::Cancelled)
        } else if allowed {
            Ok(())
        } else {
            Err(Error::PermissionDenied)
        }
    }
}

impl<PeerId> Default for OperationControl<PeerId> {
    fn default() -> Self {
        Self::new(|_, _| Box::pin(async { true }))
    }
}
