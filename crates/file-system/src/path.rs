use crate::Error;

pub fn file(path: &str) -> Result<(), Error> {
    if path.is_empty() || path.ends_with('/') {
        return Err(Error::InvalidPath);
    }
    components(path)
}

pub fn directory(path: &str) -> Result<(), Error> {
    if path.is_empty() {
        return Ok(());
    }
    components(path)
}

fn components(path: &str) -> Result<(), Error> {
    if path.starts_with('/')
        || path
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".." | ".data"))
    {
        return Err(Error::InvalidPath);
    }
    Ok(())
}
