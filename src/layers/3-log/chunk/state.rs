use super::ChunkId;

type BitMap = bitvec::prelude::BitVec<u8, bitvec::prelude::Lsb0>;

/// The persistent state of a chunk allocator.
pub struct ChunkAllocState {
    // A bitmap where each bit indicates whether a corresponding chunk has been
    // allocated.
    alloc_map: BitMap,
    // The number of free chunks.
    free_count: usize,
    // The minimum free chunk Id. Useful to narrow the scope of searching for
    // free chunk IDs.
    min_free: usize,
}
// TODO: Separate persistent and volatile state of `ChunkAlloc`

impl ChunkAllocState {
    /// Creates a persistent state for managing chunks of the specified number.
    /// Initially, all chunks are free.
    pub fn new(capacity: usize) -> Self {
        Self {
            alloc_map: BitMap::repeat(false, capacity),
            free_count: capacity,
            min_free: 0,
        }
    }

    /// Allocate a chunk, returning its ID.
    pub fn alloc(&mut self) -> Option<ChunkId> {
        let min_free = self.min_free;
        if min_free >= self.alloc_map.len() {
            return None;
        }

        let free_chunk_id = self.alloc_map[min_free..]
            .first_zero()
            .expect("there must exist a zero")
            + min_free;
        self.alloc_map.set(free_chunk_id, true);
        self.free_count -= 1;

        // Keep the invariance that all free chunk IDs are no less than min_free
        self.min_free = free_chunk_id + 1;

        Some(free_chunk_id)
    }

    /// Deallocate the chunk of a given ID.
    ///
    /// # Panic
    ///
    /// Deallocating a free chunk causes panic.
    pub fn dealloc(&mut self, chunk_id: ChunkId) {
        debug_assert!(self.alloc_map[chunk_id] == true);
        self.alloc_map.set(chunk_id, false);
        self.free_count += 1;

        // Keep the invariance that all free chunk IDs are no less than min_free
        if chunk_id < self.min_free {
            self.min_free = chunk_id;
        }
    }

    /// Returns the total number of chunks.
    pub fn capacity(&self) -> usize {
        self.alloc_map.len()
    }

    /// Returns the number of free chunks.
    pub fn free_count(&self) -> usize {
        self.free_count
    }
}
