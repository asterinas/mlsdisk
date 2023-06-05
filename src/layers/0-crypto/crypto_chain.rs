/// A cryptographically-protected chain of blocks.
/// 
/// `CrytpoChain<B>` allows writing and reading a sequence of 
/// consecutive blocks securely to and from an untrusted storage of data log 
/// `B: BlockLog`.
/// The target use case of `CrytpoChain` is to implement secure journals,
/// where old data are scanned and new data are appended.
/// 
/// # On-disk format
/// 
/// The on-disk format of each block is shown below.
/// 
/// ```text
/// ┌─────────────────────┬───────┬──────────┬──────────┬──────────┬─────────┐
/// │  Encrypted payload  │  Gap  │  Length  │  PreMac  │  CurrMac │   IV    │
/// │(Length <= 4KB - 40B)│       │   (4B)   │  (16B)   │   (16B)  │  (12B)  │
/// └─────────────────────┴───────┴──────────┴──────────┴──────────┴─────────┘
///
/// ◄─────────────────────────── Block size (4KB) ──────────────────────────►
/// ```
/// 
/// Each block begins with encrypted user payload. The size of payload
/// must be smaller than that of block size as each block ends with a footer 
/// (in plaintext).
/// The footer consists of fours parts: the length of the payload (in bytes),
/// the MAC of the previous block, the MAC of the current block, the IV used
/// for encrypting the current block.
/// The MAC of a block protects the encrypted payload, its length, and the MAC
/// of the previous block.
/// 
/// # Security
/// 
/// Each `CryptoChain` is assigned a randomly-generated encryption key. 
/// Each block is encrypted using this key and a randomly-generated IV.
/// This setup ensures the confidentiality of payload and even the same payloads
/// result in different ciphertexts.
/// 
/// `CryptoChain` is called a "chain" of blocks because each block 
/// not only stores its own MAC, but also the MAC of its previous block.
/// This effectively forms a "chain" (much like a blockchain), 
/// ensuring the orderness and consecutiveness of the sequence of blocks.
///
/// Due to this chain structure, the integrity of a `CryptoChain` can be ensured
/// by verifying the MAC of the last block. Once the integrity of the last block
/// is verified, the integrity of all previous blocks can also be verified.
pub struct CryptoChain<L> {
    block_log: L,
    key: Key,
    block_range: Range<BlockId>,
    block_macs: Vec<Mac>,
}

#[repr(C)]
#[derive(Pod)]
struct Footer {
    len: u32,
    pre_mac: Mac,
    this_mac: Mac,
    this_iv: Iv,
}

impl<L: BlockLog> CrytpoChain<L> {
    /// The available size in each chained block is smaller than that of 
    /// the block size.
    pub const AVAIL_BLOCK_SIZE: usize = BLOCK_SIZE - mem::size::<Footer>();

    /// Create a new `CryptoChain` using `block_log: L` as the storage.
    /// 
    /// The first block of the new `CryptoChain` is specified by `first_block`.
    /// The length of `first_block` must be equal to `AVAIL_BLOCK_SIZE`.
    pub fn create(block_log: L, first_block: &[u8]) -> Result<Self> {
        todo!()
    }

    /// Recover an existing `CryptoChain` backed by `block_log: L`, 
    /// starting from its `from` block.
    pub fn recover(key: Key, block_log: L, from: BlockId) -> Recovery<L> {
        Recovery::new(block_log, key, from)
    }

    /// Read a block at a specified position.
    /// 
    /// The length of the given buffer must not be equal to `AVAIL_BLOCK_SIZE`.
    /// 
    /// # Security
    /// 
    /// The authenticity of the block is guaranteed.
    pub fn read(&self, pos: BlockId, buf: &mut [u8]) -> Result<()> {
        todo!()
    }

    /// Append a block at the end.
    /// 
    /// The length of the given buffer must not be equal to `AVAIL_BLOCK_SIZE`.
    /// 
    /// # Security
    /// 
    /// The confidentiality of the block is guaranteed.
    pub fn append(&mut self, buf: &[u8]) -> Result<()> {
        todo!()
    }

    /// Ensures the persistence of data.
    pub fn flush(&self) -> Result<()> {
        todo!()
    }

    /// Trim the blocks before a specified position (exclusive).
    /// 
    /// The purpose of this method is to free some memory used for keeping the 
    /// MACs of accessible blocks. After trimming, the range of accessible 
    /// blocks is shrinked accordingly.
    pub fn trim(&mut self, before_block: BlockId) {
        // We must ensure the invariance that there are at least one valid block
        // after trimming.
        debug_assert!(before_block < self.block_range.end);

        if before_block <= self.block_range.start {
            return;
        }

        let num_blocks_trimmed = before_block - self.block_range.start;
        self.block_range.start = before_block;
        self.block_macs.drain(..num_blocks_trimmed);
    }

    /// Returns the range of blocks that are accessible through the `CryptoChain`.
    pub fn block_range(&self) -> Range<BlockId> {
        self.block_range
    }

    /// Returns the number of blocks that are accessible through the `CryptoChain`.
    pub fn num_blocks(&self) -> usize {
        self.block_range.len()
    }

    /// Returns the encryption key of the `CryptoChain`.
    pub fn key(&self) -> &Key {
        &self.key
    }
}

/// `Recovery<L>` represents an instance `CryptoChain<L>` being recovered.
/// 
/// An object `Recovery<L>` attempts to recover as many valid blocks of 
/// a `CryptoChain` as possible. A block is valid if and only if its real MAC
/// is equal to the MAC value recorded in its successor.
/// 
/// For the last block, which does not have a successor block, the user
/// can obtain its MAC from `Recovery<L>` and verify the MAC by comparing it
/// with an expected value from another trusted source.
/// 
/// After recovering the valid range of blocks with `Recovery<L>`, it can be
/// consumed to obtain a `CrytpoChain<L>` object.
pub struct Recovery<L> {
    block_log: L,
    key: Key,
    block_range: Range<BlockId>,
    block_macs: Vec<Mac>,
    block_buf: BlockBuf<Box<[u8]>>,
}

impl<L: BlockLog> Recovery<L> {
    pub fn new(block_log: L, key: Key, first_block: BlockId) -> Self {
        let block_range = (first_block..first_block);
        let block_macs = Vec::new();
        Self {
            block_log,
            key,
            block_range,
            block_macs,
        }
    }

    /// Returns the number of valid blocks.
    /// 
    /// Each success call to `next` increments the number of valid blocks.
    pub fn num_blocks(&self) -> usize {
        self.block_range.len()
    }

    /// Returns the range of valid blocks.
    /// 
    /// Each success call to `next` increments the upper bound by one.
    pub fn block_range(&self) -> &Block<BlockId> {
        &self.block_range
    }

    /// Returns the MACs of valid blocks.
    /// 
    /// Each success call to `next` pushes the MAC of the new valid block.
    pub fn block_macs(&self) -> &[Mac] {
        &self.block_macs
    }

    /// Open a `CryptoChain<L>` from the recovery object.
    pub fn open(self) -> Arc<CryptoChain<L>> {
        todo!()
    }
}

impl<L> Iterator<Item = &Mac> for Recovery<L> {
    fn next(&self) -> Option<&Mac> {
        let (next_block_mac, curr_block_mac) = {
            let next_block_id = block_range.end;
            self.block_log.read(next_block_id, &mut self.block_buf).ok()?;
            todo!("decrypt and validate the block")
        };

        // Crytpo blocks are chained: each block stores not only
        // the MAC of its own, but also the MAC of its previous block.
        // So we need to check whether the two MAC values are the same.
        if Some(last_block_mac) = self.block_macs.last() {
            if last_block_mac != curr_block_mac {
                return None;
            }
        }

        self.block_range.end += 1;
        self.block_macs.push_back(next_block_mac);

        Ok(next_block_mac)
    }
}


/// The available size of each journal block, which is a little bit smaller
/// than the size of a block.
pub const BLOCK_AVAIL_SIZE: usize = {
    const BLOCK_SIZE: usize = 4096;
    BLOCK_SIZE - core::mem::size_of::<Footer>()
}

/// The footer of each journal block.
struct Footer {
    prev_mac: Mac,
    this_id: BlockId,
    this_mac: Mac,
}
