use alloc::vec::Vec;
use bittle::{BigEndian, Bits, BitsMut};
use core::ops::Index;
use serde::{Deserialize, Serialize};

/// A compact array of bits.
///
/// The bitmap is backed by `Vec<u64>`, and its default endianness is _BigEndian_.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BitMap {
    bits: Vec<u64>,
    len: usize,
}

impl BitMap {
    /// The one bit represents `true`.
    const ONE: bool = true;

    /// The zero bit represents `false`.
    const ZERO: bool = false;

    /// Create a new `BitMap` by repeating a bit for the desired length.
    pub fn repeat(value: bool, len: usize) -> Self {
        let vec_len = (len + 64 - 1) / 64;
        let mut bits = Vec::with_capacity(vec_len);
        if value == Self::ONE {
            bits.resize(vec_len, !0u64);
        } else {
            bits.resize(vec_len, 0u64);
        }

        // Set the unused bits in the last u64 with zero.
        if len % 64 != 0 {
            let mask = (1u64 << (len % 64)) - 1;
            bits[vec_len - 1] &= mask;
        }
        Self { bits, len }
    }

    /// Return the total number of bits.
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    fn check_index(&self, index: usize) {
        if index >= self.len() {
            panic!(
                "bitmap index {} is out of range, total bits {}",
                index, self.len,
            );
        }
    }

    /// Test if the given bit is set using `BigEndian` indexing.
    pub fn test_bit(&self, index: usize) -> bool {
        self.check_index(index);
        self.bits.test_bit_in::<BigEndian>(index as _)
    }

    /// Set the given bit using `BigEndian` indexing.
    pub fn set_bit(&mut self, index: usize) {
        self.check_index(index);
        self.bits.set_bit_in::<BigEndian>(index as _);
    }

    /// Clear the given bit using `BigEndian` indexing.
    pub fn clear_bit(&mut self, index: usize) {
        self.check_index(index);
        self.bits.clear_bit_in::<BigEndian>(index as _)
    }

    /// Set the given bit with `value`, using `BigEndian` indexing.
    pub fn set(&mut self, index: usize, value: bool) {
        if value == Self::ONE {
            self.set_bit(index);
        } else {
            self.clear_bit(index);
        }
    }

    #[inline]
    fn bits_not_in_use(&self) -> usize {
        self.bits.len() * 64 - self.len
    }

    /// Get the number of ones in the bitmap.
    pub fn count_ones(&self) -> usize {
        self.bits.count_ones() as _
    }

    /// Get the number of zeros in the bitmap.
    pub fn count_zeros(&self) -> usize {
        let mut total_zeros = self.bits.count_zeros() as usize;
        total_zeros - self.bits_not_in_use()
    }

    /// Find the index of the first one bit in the bitmap.
    ///
    /// Search begins with the u64 (BigEndian) of `from` index (inclusively),
    /// return `None` if there is no one bit.
    pub fn first_one(&self, from: usize) -> Option<usize> {
        let first_u64_index = from / 64;

        self.bits[first_u64_index..]
            .iter_ones()
            .map(|index| first_u64_index * 64 + (index as usize))
            .find(|&index| index >= from)
    }

    /// Find the index of the first zero bit in the bitmap.
    ///
    /// Search begins with the u64 (BigEndian) of `from` index (inclusively),
    /// return `None` if there is no zero bit.
    pub fn first_zero(&self, from: usize) -> Option<usize> {
        let first_u64_index = from / 64;

        self.bits[first_u64_index..]
            .iter_zeros()
            .map(|index| first_u64_index * 64 + (index as usize))
            .find(|&index| index >= from && index < self.len())
    }
}

impl Index<usize> for BitMap {
    type Output = bool;

    fn index(&self, index: usize) -> &Self::Output {
        if self.test_bit(index) {
            &BitMap::ONE
        } else {
            &BitMap::ZERO
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BitMap;

    #[test]
    fn bitmap() {
        let mut bm = BitMap::repeat(false, 100);
        assert_eq!(bm.len(), 100);

        bm.set_bit(4);
        assert_eq!(bm.bits[0], 0x10);

        bm.clear_bit(4);
        bm.set(8, true);
        assert_eq!(bm.bits[0], 0x100);
        assert_eq!(bm.test_bit(4), false);
        assert_eq!(bm.test_bit(8), true);

        bm.set(64, true);
        assert_eq!(bm.bits[1], 0x1);

        assert_eq!(bm.count_ones(), 2);
        assert_eq!(bm.count_zeros(), 98);
        assert_eq!(bm.first_one(8), Some(8));
        assert_eq!(bm.first_one(32), Some(64));
        assert_eq!(bm.first_one(65), None);

        let mut bm = BitMap::repeat(true, 100);
        assert_eq!(bm.count_ones(), 100);
        assert_eq!(bm.count_zeros(), 0);
        bm.set(64, false);
        assert_eq!(bm.first_zero(0), Some(64));
        assert_eq!(bm.first_zero(65), None);
    }
}
