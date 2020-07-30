use std::convert::TryInto;

use super::VersionedValue;

#[derive(Debug)]
pub struct VersionedStorage {
    pub(crate) db: sled::Db,
}

impl VersionedStorage {
    pub(crate) fn get(&self, key: &[u8]) -> Option<VersionedValue> {
        let raw_value = self.db.get(key).expect("db io issue")?;

        let ballot = u64::from_le_bytes(raw_value[..8].try_into().unwrap());

        Some(VersionedValue {
            ballot,
            value: if raw_value[8..].is_empty() {
                None
            } else {
                Some(raw_value[8..].to_vec())
            },
        })
    }

    pub(crate) fn update_if_newer(
        &self,
        key: &[u8],
        proposal: VersionedValue,
    ) -> Result<(), u64> {
        // we use a sled transaction to push all concurrency concerns into the db,
        // so we can be as massively concurrent as we desire in the rest of the
        // server code
        self.db
            .transaction::<_, _, ()>(|db| {
                let raw_value_opt = db.get(&key).expect("db io issue");

                let current_ballot = if let Some(ref raw_value) = raw_value_opt
                {
                    let current_ballot =
                        u64::from_le_bytes(raw_value[..8].try_into().unwrap());
                    current_ballot
                } else {
                    0
                };

                if proposal.ballot > current_ballot {
                    let mut serialized: Vec<u8> =
                        proposal.ballot.to_le_bytes().to_vec();

                    if let Some(value) = &proposal.value {
                        serialized.extend_from_slice(value);
                    }

                    db.insert(&*key, serialized)?;
                    //db.flush();

                    Ok(Ok(()))
                } else {
                    Ok(Err(current_ballot))
                }
            })
            .expect("db io error")
    }
}
