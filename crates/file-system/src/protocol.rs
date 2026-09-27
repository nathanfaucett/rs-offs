use serde::{Serialize, de::DeserializeOwned};

use crate::{Error, SyncMessage};

pub const FILESYSTEM_SYNC_PROTOCOL_VERSION: u8 = 1;
pub const MAX_FILESYSTEM_SYNC_FRAME_SIZE: usize = 8 * 1024 * 1024;

pub fn encode_sync_message<PeerId>(message: &SyncMessage<PeerId>) -> Result<Vec<u8>, Error>
where
    PeerId: Serialize,
{
    let payload = postcard::to_allocvec(message)
        .map_err(|error| Error::InvalidSyncFrame(error.to_string()))?;
    if payload.len() > MAX_FILESYSTEM_SYNC_FRAME_SIZE - 1 {
        return Err(Error::SyncFrameTooLarge);
    }
    let mut frame = Vec::with_capacity(payload.len() + 1);
    frame.push(FILESYSTEM_SYNC_PROTOCOL_VERSION);
    frame.extend_from_slice(&payload);
    Ok(frame)
}

pub fn decode_sync_message<PeerId>(frame: &[u8]) -> Result<SyncMessage<PeerId>, Error>
where
    PeerId: DeserializeOwned,
{
    if frame.len() > MAX_FILESYSTEM_SYNC_FRAME_SIZE {
        return Err(Error::SyncFrameTooLarge);
    }
    let (&version, payload) = frame
        .split_first()
        .ok_or(Error::InvalidSyncFrame("empty frame".to_owned()))?;
    if version != FILESYSTEM_SYNC_PROTOCOL_VERSION {
        return Err(Error::UnsupportedSyncProtocolVersion(version));
    }
    postcard::from_bytes(payload).map_err(|error| Error::InvalidSyncFrame(error.to_string()))
}

#[cfg(test)]
mod tests {
    use crate::SyncMessage;

    use super::{
        FILESYSTEM_SYNC_PROTOCOL_VERSION, MAX_FILESYSTEM_SYNC_FRAME_SIZE, decode_sync_message,
        encode_sync_message,
    };

    #[test]
    fn sync_message_round_trips_with_protocol_version() {
        let message = SyncMessage::SnapshotRequest { peer: 7_u8 };
        let frame = encode_sync_message(&message).expect("encode sync message");
        assert_eq!(frame[0], FILESYSTEM_SYNC_PROTOCOL_VERSION);
        assert_eq!(
            decode_sync_message::<u8>(&frame).expect("decode sync message"),
            message
        );
    }

    #[test]
    fn rejects_invalid_and_unsupported_frames() {
        assert!(decode_sync_message::<u8>(&[]).is_err());
        assert!(decode_sync_message::<u8>(&[0]).is_err());
        assert!(decode_sync_message::<u8>(&[FILESYSTEM_SYNC_PROTOCOL_VERSION, 0xff]).is_err());
    }

    #[test]
    fn rejects_oversized_frames_before_decoding() {
        let frame = vec![FILESYSTEM_SYNC_PROTOCOL_VERSION; MAX_FILESYSTEM_SYNC_FRAME_SIZE + 1];
        assert!(decode_sync_message::<u8>(&frame).is_err());
    }
}
