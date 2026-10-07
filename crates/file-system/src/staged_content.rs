use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
};

use uuid::Uuid;

use crate::Error;

pub(crate) struct StagedContent {
    pub(crate) file: File,
    temporary: PathBuf,
    destination: PathBuf,
    published: bool,
    committed: bool,
}

impl StagedContent {
    pub(crate) fn new(destination: PathBuf) -> Result<Self, Error> {
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
            temporary,
            destination,
            published: false,
            committed: false,
        })
    }

    pub(crate) fn copy_from(&mut self, source: &Path) -> Result<(), Error> {
        std::io::copy(&mut File::open(source)?, &mut self.file)?;
        Ok(())
    }

    pub(crate) fn publish(&mut self) -> Result<(), Error> {
        fs::rename(&self.temporary, &self.destination)?;
        self.published = true;
        Ok(())
    }

    pub(crate) fn commit(mut self) {
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
