use std::{
    io::{Error, ErrorKind},
    sync::Arc,
};

use iroh::{EndpointId, endpoint::Connection};
use noq::SendStream;
use tokio::sync::Mutex;

pub const MAX_PAIRING_PAYLOAD_LENGTH: usize = 4096;

#[derive(Clone, Debug)]
pub struct PairingOffer {
    pub remote_id: EndpointId,
    pub payload: Vec<u8>,
    response: Arc<Mutex<Option<SendStream>>>,
    _connection: Connection,
}

impl PairingOffer {
    pub(crate) fn new(
        remote_id: EndpointId,
        payload: Vec<u8>,
        response: SendStream,
        connection: Connection,
    ) -> Self {
        Self {
            remote_id,
            payload,
            response: Arc::new(Mutex::new(Some(response))),
            _connection: connection,
        }
    }

    pub async fn reply(&self, payload: &[u8]) -> Result<(), Error> {
        validate_payload(payload)?;
        let mut response = self.response.lock().await.take().ok_or_else(|| {
            Error::new(
                ErrorKind::BrokenPipe,
                "pairing offer has already been answered",
            )
        })?;
        response.write_all(payload).await.map_err(Error::other)?;
        response.finish().map_err(Error::other)
    }
}

pub type PairingEvent = PairingOffer;

pub(crate) fn validate_payload(payload: &[u8]) -> Result<(), Error> {
    if payload.len() > MAX_PAIRING_PAYLOAD_LENGTH {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "pairing payload is too large",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{MAX_PAIRING_PAYLOAD_LENGTH, validate_payload};

    #[test]
    fn limits_pairing_payloads() {
        assert!(validate_payload(&[0; MAX_PAIRING_PAYLOAD_LENGTH]).is_ok());
        assert!(validate_payload(&[0; MAX_PAIRING_PAYLOAD_LENGTH + 1]).is_err());
    }
}
