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

    pub(crate) fn update_if_newer(
        &self,
        key: Vec<u8>,
        proposal: VersionedValue,
    ) -> Result<(), VersionedValue> {
        // we use a sled transaction to push all concurrency concerns into the db,
        // so we can be as massively concurrent as we desire in the rest of the
        // server code
        let ret = self
            .db
            .transaction::<_, _, ()>(|db| {
                let raw_value_opt = db.get(&key).expect("db io issue");

                let (current_ballot, current_value) =
                    if let Some(ref raw_value) = raw_value_opt {
                        let (zc_ballot, value_bytes): (
                            LayoutVerified<&[u8], U64<LittleEndian>>,
                            &[u8],
                        ) = LayoutVerified::new_from_prefix(&**raw_value)
                            .unwrap();
                        let current_ballot = zc_ballot.get();
                        let current_value = if value_bytes.is_empty() {
                            None
                        } else {
                            Some(value_bytes.to_vec())
                        };
                        (current_ballot, current_value)
                    } else {
                        (0, None)
                    };

                if proposal.ballot > current_ballot {
                    let zc_u64: U64<LittleEndian> = U64::new(proposal.ballot);
                    let mut serialized: Vec<u8> = zc_u64.as_bytes().to_vec();

                    if let Some(value) = &proposal.value {
                        serialized.extend_from_slice(value);
                    }

                    db.insert(&*key, serialized)?;

                    Ok(Ok(()))
                } else {
                    Ok(Err(VersionedValue {
                        ballot: current_ballot,
                        value: current_value,
                    }))
                }
            })
            .expect("db io issue");

        // fsync our result before communicating anything to the client
        self.db.flush().expect("db io issue");

        ret
    }
}
