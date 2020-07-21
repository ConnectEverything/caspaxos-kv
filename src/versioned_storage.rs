use {
    byteorder::LittleEndian,
    zerocopy::{byteorder::U64, AsBytes, LayoutVerified},
};

use super::VersionedValue;

#[derive(Debug)]
pub struct VersionedStorage {
    pub(crate) db: sled::Db,
}

impl VersionedStorage {
    pub(crate) fn get(&self, key: &[u8]) -> Option<VersionedValue> {
        let raw_value = self.db.get(key).expect("db io issue")?;

        let (ballot, value_bytes): (
            LayoutVerified<&[u8], U64<LittleEndian>>,
            &[u8],
        ) = LayoutVerified::new_from_prefix(&*raw_value).unwrap();

        Some(VersionedValue {
            ballot: ballot.get(),
            value: if value_bytes.is_empty() {
                None
            } else {
                Some(value_bytes.to_vec())
            },
        })
    }

    pub(crate) fn insert(&self, key: Vec<u8>, vv: VersionedValue) {
        let zc_u64: U64<LittleEndian> = U64::new(vv.ballot);
        let mut serialized: Vec<u8> = zc_u64.as_bytes().to_vec();
        if let Some(value) = vv.value {
            serialized.extend_from_slice(&value);
        }
        self.db.insert(key, serialized).expect("db io issue");
        self.db.flush().expect("db io issue");
    }
}
