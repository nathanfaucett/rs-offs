use std::{fs::File, io::Read};

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

impl Iterator for ReadStream {
    type Item = Result<Vec<u8>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunk = vec![0; self.chunk_size];
        match self.file.read(&mut chunk) {
            Ok(0) => None,
            Ok(len) => {
                chunk.truncate(len);
                Some(Ok(chunk))
            }
            Err(error) => Some(Err(error.into())),
        }
    }
}
