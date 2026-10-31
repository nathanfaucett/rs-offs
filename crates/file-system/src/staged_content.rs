use std::{
    fs::{self, File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::PathBuf,
};

use ofdb_platform::BoxFuture;
use uuid::Uuid;

use crate::{Error, content::Staged};

pub(crate) struct StagedContent {
    file: File,
    content_root: PathBuf,
    temporary: PathBuf,
    destination: PathBuf,
    published: bool,
    committed: bool,
}

impl StagedContent {
    pub(crate) fn new(content_root: PathBuf, destination: PathBuf) -> Result<Self, Error> {
        fs::create_dir_all(destination.parent().expect("content has a parent"))?;
        let temporary = destination.with_extension(format!("{}.tmp", Uuid::now_v7()));
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(&temporary)
            .inspect_err(|_| {
                let _ = fs::remove_dir(destination.parent().expect("content has a parent"));
            })?;
        Ok(Self {
            file,
            content_root,
            temporary,
            destination,
            published: false,
            committed: false,
        })
    }

    fn seek(&mut self, position: SeekFrom) -> Result<(), Error> {
        self.file.seek(position)?;
        Ok(())
    }
}

impl Staged for StagedContent {
    fn write<'a>(&'a mut self, bytes: &'a [u8]) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async { self.file.write_all(bytes).map_err(Error::from) })
    }

    fn copy_from<'a>(
        &'a mut self,
        file_id: Uuid,
        revision: Uuid,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let source = self
                .content_root
                .join(file_id.to_string())
                .join(revision.to_string());
            std::io::copy(&mut File::open(source)?, &mut self.file)?;
            Ok(())
        })
    }

    fn write_at<'a>(
        &'a mut self,
        offset: u64,
        bytes: &'a [u8],
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.seek(SeekFrom::Start(offset))?;
            self.file.write_all(bytes).map_err(Error::from)
        })
    }

    fn write_end<'a>(&'a mut self, bytes: &'a [u8]) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.seek(SeekFrom::End(0))?;
            self.file.write_all(bytes).map_err(Error::from)
        })
    }

    fn sync<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async { self.file.sync_all().map_err(Error::from) })
    }

    fn publish<'a>(&'a mut self) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            fs::rename(&self.temporary, &self.destination)?;
            self.published = true;
            Ok(())
        })
    }

    fn commit(&mut self) {
        self.committed = true;
    }
}

impl Drop for StagedContent {
    fn drop(&mut self) {
        if !self.committed {
            let path = if self.published {
                &self.destination
            } else {
                &self.temporary
            };
            let _ = fs::remove_file(path);
            let _ = fs::remove_dir(self.destination.parent().expect("content has a parent"));
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ContentStep {
    Write,
    Sync,
    Publish,
    Metadata,
}

#[cfg(test)]
pub(crate) type ContentAction = (ContentStep, Box<dyn FnOnce() + Send>);
