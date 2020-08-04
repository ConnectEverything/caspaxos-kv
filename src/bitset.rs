#![allow(unused)]

pub(crate) struct Bitset(u64);

impl Bitset {
    pub(crate) fn none() -> Bitset {
        Bitset(0)
    }

    pub(crate) fn all() -> Bitset {
        Bitset(u64::MAX)
    }

    pub(crate) fn add(&mut self, item: usize) {
        assert!(item < 64, "bitsets can only hold up to value 63, are you using more servers than that?");
        self.0 |= 1 << item;
    }
}

impl Iterator for Bitset {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        println!("self before: {:b}", self.0);
        let tz = self.0.trailing_zeros() as usize;
        println!("tz: {}", tz);
        if tz == 64 {
            return None;
        }

        self.0 ^= 1 << tz;
        println!("self after: {:b}", self.0);

        Some(tz)
    }
}

#[test]
fn test_bitset() {}

#[cfg(test)]
mod qc {
    use std::collections::HashSet;

    use quickcheck::{Arbitrary, Gen};
    use rand::Rng;

    use super::*;

    #[derive(Debug, Clone, Copy)]
    struct U6(usize);

    impl Arbitrary for U6 {
        fn arbitrary<G: Gen>(g: &mut G) -> U6 {
            U6(g.gen_range(0, 64))
        }
    }

    quickcheck::quickcheck! {
        fn bitset(items: Vec<U6>) -> bool {
            let expected: HashSet<usize> = items.iter().map(|u6| u6.0).collect();

            let mut bitset = Bitset::none();

            for item in items {
                bitset.add(item.0);
            }

            assert_eq!(bitset.collect::<HashSet<usize>>(), expected);
            true
        }
    }
}
