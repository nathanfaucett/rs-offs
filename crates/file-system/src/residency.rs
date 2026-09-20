use std::collections::BTreeMap;

use crate::{Error, path::directory};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Residency {
    Full,
    #[default]
    Passthrough,
}

#[derive(Debug, Default)]
pub(crate) struct ResidencyRules {
    rules: BTreeMap<String, Residency>,
}

impl ResidencyRules {
    pub(crate) fn get(&self, path: &str) -> Result<Residency, Error> {
        directory(path)?;
        let mut current = path;
        loop {
            if let Some(residency) = self.rules.get(current) {
                return Ok(*residency);
            }
            current = match current.rsplit_once('/') {
                Some((parent, _)) => parent,
                None => return Ok(self.rules.get("").copied().unwrap_or_default()),
            };
        }
    }

    pub(crate) fn set(&mut self, path: &str, residency: Residency) -> Result<(), Error> {
        directory(path)?;
        self.rules.insert(path.to_owned(), residency);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Residency, ResidencyRules};

    #[test]
    fn uses_the_longest_matching_path_rule() {
        let mut rules = ResidencyRules::default();
        rules.set("", Residency::Full).unwrap();
        rules.set("notes", Residency::Passthrough).unwrap();
        rules.set("notes/today.txt", Residency::Full).unwrap();

        assert_eq!(rules.get("other.txt").unwrap(), Residency::Full);
        assert_eq!(
            rules.get("notes/other.txt").unwrap(),
            Residency::Passthrough
        );
        assert_eq!(rules.get("notes/today.txt").unwrap(), Residency::Full);
    }
}
