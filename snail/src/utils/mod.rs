use rand::random;
use std::{fs, path::Path};
use tracing::debug;

use crate::{torrent::HashId, Result};

pub fn load_id(id_path: &Path) -> Result<HashId> {
    if id_path.exists() {
        let id = fs::read(id_path)?;
        assert_eq!(id.len(), 20);
        return HashId::from_slice(&id);
    }
    let id: [u8; 20] = random();
    debug!(id = ?id, id_path = ?id_path, "generate new id");
    fs::write(id_path, id)?;
    Ok(id.into())
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::env;

    #[test]
    fn test_load_ip() {
        let p = env::temp_dir().join("test-hash-id");
		let id = load_id(&p).unwrap();
		assert_eq!(id.len(), 20);
		assert_eq!(id, load_id(&p).unwrap());
    }
}
