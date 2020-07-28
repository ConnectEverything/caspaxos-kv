use std::convert::TryFrom;

use crate::{Envelope, Message, Request, Response, VersionedValue};

/// Items that may be serialized and deserialized
pub trait Serialize: Sized {
    /// Returns the buffer size required to hold
    /// the serialized bytes for this item.
    fn serialized_size(&self) -> usize;

    /// Serializees the item without allocating.
    ///
    /// # Panics
    ///
    /// Panics if the buffer is not large enough.
    fn serialize_into(&self, buf: &mut &mut [u8]);

    /// Attempts to deserialize this type from some bytes.
    fn deserialize(buf: &mut &[u8]) -> Result<Self, ()>;

    /// Returns owned serialized bytes.
    fn serialize(&self) -> Vec<u8> {
        let sz = self.serialized_size();
        let mut buf = vec![0; usize::try_from(sz).unwrap()];
        self.serialize_into(&mut buf.as_mut_slice());
        buf
    }
}

// Moves a reference to mutable bytes forward,
// sidestepping Rust's limitations in reasoning
// about lifetimes.
//
// ☑ Checked with Miri by Tyler on 2019-12-12
#[allow(unsafe_code)]
fn scoot(buf: &mut &mut [u8], amount: usize) {
    assert!(buf.len() >= amount);
    let len = buf.len();
    let ptr = buf.as_mut_ptr();
    let new_len = len - amount;

    unsafe {
        let new_ptr = ptr.add(amount);
        *buf = std::slice::from_raw_parts_mut(new_ptr, new_len);
    }
}

impl Serialize for VersionedValue {
    fn serialized_size(&self) -> usize {
        self.ballot.serialized_size() + self.value.serialized_size()
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        self.ballot.serialize_into(buf);
        self.value.serialize_into(buf);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<VersionedValue, ()> {
        Ok(VersionedValue {
            ballot: Serialize::deserialize(buf)?,
            value: Serialize::deserialize(buf)?,
        })
    }
}

impl Serialize for uuid::Uuid {
    fn serialized_size(&self) -> usize {
        16
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        let bytes = self.as_bytes();
        buf[..16].copy_from_slice(bytes);
        scoot(buf, 16);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<uuid::Uuid, ()> {
        if buf.len() < 16 {
            return Err(());
        }
        let value = uuid::Uuid::from_slice(&buf[..16]).map_err(|_| ())?;
        *buf = &buf[16..];
        Ok(value)
    }
}

impl Serialize for bool {
    fn serialized_size(&self) -> usize {
        1
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        let byte = u8::from(*self);
        buf[0] = byte;
        scoot(buf, 1);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<bool, ()> {
        if buf.is_empty() {
            return Err(());
        }
        let value = buf[0] != 0;
        *buf = &buf[1..];
        Ok(value)
    }
}

impl Serialize for Envelope {
    fn serialized_size(&self) -> usize {
        let discriminant_size = 1;
        let uuid_size = 16;
        let message_size = match &self.message {
            Message::Request(Request::Ping) => 0,
            Message::Request(Request::Prepare { ballot, key }) => {
                ballot.serialized_size() + key.serialized_size()
            }
            Message::Request(Request::Accept { key, value }) => {
                key.serialized_size() + value.serialized_size()
            }
            Message::Response(Response::Pong) => 0,
            Message::Response(Response::Promise { success }) => {
                success.serialized_size()
            }
            Message::Response(Response::Accepted { success }) => {
                if let Err(value) = success {
                    false.serialized_size() + value.serialized_size()
                } else {
                    true.serialized_size()
                }
            }
        };

        discriminant_size + uuid_size + message_size
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        match &self.message {
            Message::Request(Request::Ping) => {
                1u8.serialize_into(buf);
                self.uuid.serialize_into(buf);
            }
            Message::Request(Request::Prepare { ballot, key }) => {
                2u8.serialize_into(buf);
                self.uuid.serialize_into(buf);
                ballot.serialize_into(buf);
                key.serialize_into(buf);
            }
            Message::Request(Request::Accept { key, value }) => {
                3u8.serialize_into(buf);
                self.uuid.serialize_into(buf);
                key.serialize_into(buf);
                value.serialize_into(buf);
            }
            Message::Response(Response::Pong) => {
                4u8.serialize_into(buf);
                self.uuid.serialize_into(buf);
            }
            Message::Response(Response::Promise { success }) => {
                5u8.serialize_into(buf);
                self.uuid.serialize_into(buf);
                success.serialize_into(buf);
            }
            Message::Response(Response::Accepted { success }) => {
                6u8.serialize_into(buf);
                self.uuid.serialize_into(buf);
                if let Err(value) = success {
                    false.serialize_into(buf);
                    value.serialize_into(buf);
                } else {
                    true.serialize_into(buf);
                }
            }
        }
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Envelope, ()> {
        let discriminant = buf[0];
        let uuid = if let Ok(uuid) = uuid::Uuid::from_slice(&buf[1..17]) {
            uuid
        } else {
            eprintln!("malformed uuid");
            return Err(());
        };

        *buf = &buf[17..];

        let message = match discriminant {
            1 => {
                // Ping
                Message::Request(Request::Ping)
            }
            2 => {
                // Prepare
                Message::Request(Request::Prepare {
                    ballot: u64::deserialize(buf)?,
                    key: Serialize::deserialize(buf)?,
                })
            }
            3 => {
                // Accept
                Message::Request(Request::Accept {
                    key: Serialize::deserialize(buf)?,
                    value: Serialize::deserialize(buf)?,
                })
            }
            4 => {
                // Pong
                Message::Response(Response::Pong)
            }
            5 => {
                // Promise
                Message::Response(Response::Promise {
                    success: Serialize::deserialize(buf)?,
                })
            }
            6 => {
                // Accepted
                let is_ok = bool::deserialize(buf)?;
                let success = if is_ok {
                    Ok(())
                } else {
                    let vv = VersionedValue::deserialize(buf)?;
                    Err(vv)
                };

                Message::Response(Response::Accepted { success })
            }
            _ => return Err(()),
        };

        Ok(Envelope { uuid, message })
    }
}

impl<A: Serialize, B: Serialize> Serialize for Result<A, B> {
    fn serialized_size(&self) -> usize {
        match self {
            Ok(a) => true.serialized_size() + a.serialized_size(),
            Err(b) => false.serialized_size() + b.serialized_size(),
        }
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        match self {
            Ok(a) => {
                true.serialize_into(buf);
                a.serialize_into(buf);
            }
            Err(b) => {
                false.serialize_into(buf);
                b.serialize_into(buf);
            }
        }
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Result<A, B>, ()> {
        let is_ok = bool::deserialize(buf)?;
        if is_ok {
            Ok(Ok(Serialize::deserialize(buf)?))
        } else {
            Ok(Err(Serialize::deserialize(buf)?))
        }
    }
}

impl<T: Serialize> Serialize for Option<T> {
    fn serialized_size(&self) -> usize {
        if let Some(v) = self {
            v.serialized_size() + true.serialized_size()
        } else {
            false.serialized_size()
        }
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        if let Some(v) = self {
            true.serialize_into(buf);
            v.serialize_into(buf);
        } else {
            false.serialize_into(buf);
        }
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Option<T>, ()> {
        let is_some = bool::deserialize(buf)?;
        if is_some {
            Ok(Some(Serialize::deserialize(buf)?))
        } else {
            Ok(None)
        }
    }
}

impl Serialize for u8 {
    fn serialized_size(&self) -> usize {
        1
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        buf[0] = *self;
        scoot(buf, 1);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<u8, ()> {
        if buf.is_empty() {
            return Err(());
        }
        let value = buf[0];
        *buf = &buf[1..];
        Ok(value)
    }
}

impl Serialize for Vec<u8> {
    fn serialized_size(&self) -> usize {
        let len = self.len();
        len + (len as u64).serialized_size()
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        (self.len() as u64).serialize_into(buf);
        buf[..self.len()].copy_from_slice(self.as_ref());
        scoot(buf, self.len());
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Vec<u8>, ()> {
        let k_len = usize::try_from(u64::deserialize(buf)?)
            .expect("should never store items that rust can't natively index");
        let ret = &buf[..k_len];
        *buf = &buf[k_len..];
        Ok(ret.into())
    }
}

impl Serialize for u64 {
    fn serialized_size(&self) -> usize {
        if *self <= 240 {
            1
        } else if *self <= 2287 {
            2
        } else if *self <= 67823 {
            3
        } else if *self <= 0x00FF_FFFF {
            4
        } else if *self <= 0xFFFF_FFFF {
            5
        } else if *self <= 0x00FF_FFFF_FFFF {
            6
        } else if *self <= 0xFFFF_FFFF_FFFF {
            7
        } else if *self <= 0x00FF_FFFF_FFFF_FFFF {
            8
        } else {
            9
        }
    }

    fn serialize_into(&self, buf: &mut &mut [u8]) {
        let sz = if *self <= 240 {
            buf[0] = u8::try_from(*self).unwrap();
            1
        } else if *self <= 2287 {
            buf[0] = u8::try_from((*self - 240) / 256 + 241).unwrap();
            buf[1] = u8::try_from((*self - 240) % 256).unwrap();
            2
        } else if *self <= 67823 {
            buf[0] = 249;
            buf[1] = u8::try_from((*self - 2288) / 256).unwrap();
            buf[2] = u8::try_from((*self - 2288) % 256).unwrap();
            3
        } else if *self <= 0x00FF_FFFF {
            buf[0] = 250;
            let bytes = self.to_le_bytes();
            buf[1..4].copy_from_slice(&bytes[..3]);
            4
        } else if *self <= 0xFFFF_FFFF {
            buf[0] = 251;
            let bytes = self.to_le_bytes();
            buf[1..5].copy_from_slice(&bytes[..4]);
            5
        } else if *self <= 0x00FF_FFFF_FFFF {
            buf[0] = 252;
            let bytes = self.to_le_bytes();
            buf[1..6].copy_from_slice(&bytes[..5]);
            6
        } else if *self <= 0xFFFF_FFFF_FFFF {
            buf[0] = 253;
            let bytes = self.to_le_bytes();
            buf[1..7].copy_from_slice(&bytes[..6]);
            7
        } else if *self <= 0x00FF_FFFF_FFFF_FFFF {
            buf[0] = 254;
            let bytes = self.to_le_bytes();
            buf[1..8].copy_from_slice(&bytes[..7]);
            8
        } else {
            buf[0] = 255;
            let bytes = self.to_le_bytes();
            buf[1..9].copy_from_slice(&bytes[..8]);
            9
        };

        scoot(buf, sz);
    }

    fn deserialize(buf: &mut &[u8]) -> Result<Self, ()> {
        if buf.is_empty() {
            return Err(());
        }
        let (res, scoot) = match buf[0] {
            0..=240 => (u64::from(buf[0]), 1),
            241..=248 => {
                (240 + 256 * (u64::from(buf[0]) - 241) + u64::from(buf[1]), 2)
            }
            249 => (2288 + 256 * u64::from(buf[1]) + u64::from(buf[2]), 3),
            other => {
                let sz = other as usize - 247;
                let mut aligned = [0; 8];
                aligned[..sz].copy_from_slice(&buf[1..=sz]);
                (u64::from_le_bytes(aligned), sz + 1)
            }
        };
        *buf = &buf[scoot..];
        Ok(res)
    }
}

#[cfg(test)]
mod qc {
    use quickcheck::{Arbitrary, Gen};
    use rand::Rng;

    use super::*;

    #[derive(Debug, Clone)]
    struct SpreadU64(u64);

    impl Arbitrary for VersionedValue {
        fn arbitrary<G: Gen>(g: &mut G) -> VersionedValue {
            VersionedValue {
                ballot: SpreadU64::arbitrary(g).0,
                value: Arbitrary::arbitrary(g),
            }
        }
    }

    impl Arbitrary for Request {
        fn arbitrary<G: Gen>(g: &mut G) -> Request {
            match g.gen_range(1, 4) {
                1 => Request::Ping,
                2 => Request::Prepare {
                    ballot: Arbitrary::arbitrary(g),
                    key: Arbitrary::arbitrary(g),
                },
                3 => Request::Accept {
                    key: Arbitrary::arbitrary(g),
                    value: Arbitrary::arbitrary(g),
                },
                _ => unreachable!(),
            }
        }
    }

    impl Arbitrary for Response {
        fn arbitrary<G: Gen>(g: &mut G) -> Response {
            match g.gen_range(1, 4) {
                1 => Response::Pong,
                2 => Response::Promise {
                    success: Arbitrary::arbitrary(g),
                },
                3 => Response::Accepted {
                    success: Arbitrary::arbitrary(g),
                },
                _ => unreachable!(),
            }
        }
    }
    impl Arbitrary for Message {
        fn arbitrary<G: Gen>(g: &mut G) -> Message {
            if g.gen() {
                Message::Request(Arbitrary::arbitrary(g))
            } else {
                Message::Response(Arbitrary::arbitrary(g))
            }
        }
    }
    impl Arbitrary for Envelope {
        fn arbitrary<G: Gen>(g: &mut G) -> Envelope {
            let uuid = uuid::Uuid::from_u128(g.gen());

            Envelope {
                uuid,
                message: Arbitrary::arbitrary(g),
            }
        }
    }

    impl Arbitrary for SpreadU64 {
        fn arbitrary<G: Gen>(g: &mut G) -> SpreadU64 {
            let uniform = g.gen::<u64>();
            let shift = g.gen_range(0, 64);
            SpreadU64(uniform >> shift)
        }
    }

    fn prop_serialize<T>(item: T) -> bool
    where
        T: Serialize + PartialEq + Clone + std::fmt::Debug,
    {
        let mut buf = vec![0; item.serialized_size() as usize];
        let buf_ref = &mut buf.as_mut_slice();
        item.serialize_into(buf_ref);
        assert_eq!(
            buf_ref.len(),
            0,
            "round-trip failed to consume produced bytes"
        );
        assert_eq!(buf.len(), item.serialized_size() as usize,);
        let deserialized = T::deserialize(&mut buf.as_slice()).unwrap();
        if item != deserialized {
            eprintln!(
                "round-trip serialization failed. original:\n\n{:?}\n\n \
                 deserialized(serialized(original)):\n\n{:?}",
                item, deserialized
            );
            false
        } else {
            true
        }
    }

    quickcheck::quickcheck! {
        fn bool(item: bool) -> bool {
            prop_serialize(item)
        }

        fn u8(item: u8) -> bool {
            prop_serialize(item)
        }

        fn vec(item: Vec<u8>) -> bool {
            prop_serialize(item)
        }

        fn option(item: Option<Vec<u8>>) -> bool {
            prop_serialize(item)
        }

        fn envelope(item: Envelope) -> bool {
            prop_serialize(item)
        }

        fn versioned_value(item: VersionedValue) -> bool {
            prop_serialize(item)
        }

        fn u64(item: SpreadU64) -> bool {
            prop_serialize(item.0)
        }
    }
}
