
/// A cryptographically-protected log of user data blocks.
/// 
/// `CryptoLog<L>`, which is backed by an untrusted block log (`L`),
/// serves as a secure log file that supports random reads and append-only 
/// writes of data blocks. `CryptoLog<L>` encrypts the data blocks and 
/// protects them with a Merkle Hash Tree (MHT), which itself is also encrypted.
/// 
/// # Security
/// 
/// Each instance of `CryptoLog<L>` is assigned a randomly-generated root key
/// upon its creation. The root key is used to encrypt the root MHT block only.
/// Each new version of the root MHT block is encrypted with the same key, but
/// different random IVs. This arrangement ensures the confidentiality of
/// the root block.
/// 
/// After flushing a `CryptoLog<L>`, a new root MHT (as well as other MHT nodes)
/// shall be appended to the backend block log (`L`).
/// The metadata of the root MHT, including its position, encryption
/// key, IV, and MAC, must be kept by the user of `CryptoLog<L>` so that
/// he or she can use the metadata to re-open the `CryptoLog`.
/// The information contained in the metadata is sufficient to verify the 
/// integrity and freshness of the root MHT node, and thus the whole `CryptoLog`.
/// 
/// Other MHT nodes as well as data nodes are encrypted with randomly-generated,
/// unique keys. Their metadata, including its position, encryption key, IV, and
/// MAC, are kept securely in their parent MHT nodes, which are also encrypted.
/// Thus, the confidentiality and integrity of non-root nodes are protected.
/// 
/// # Performance
/// 
/// Thanks to its append-only nature, `CryptoLog<L>` avoids MHT's high 
/// performance overheads under the workload of random writes 
/// due to "cascades of updates".
/// 
/// Behind the scene, `CryptoLog<L>` keeps a cache for nodes so that frequently 
/// or lately accessed nodes can be found in the cache, avoiding the I/O 
/// and decryption cost incurred when re-reading these nodes.
/// The cache is also used for buffering new data so that multiple writes to 
/// individual nodes can be merged into a large write to the underlying block log.
/// Therefore, `CryptoLog<L>` is efficient for both reads and writes.
/// 
/// # Disk space
/// 
/// One consequence of using an append-only block log (`L`) as the backend is 
/// that `CryptoLog<L>` cannot do in-place updates to existing MHT nodes.
/// This means the new version of MHT nodes are appended to the underlying block
/// log and the invalid blocks occupied by old versions are not reclaimed.
/// 
/// But lucky for us, this block reclaimation problem is not an issue in pratice.
/// This is because a `CryptoLog<L>` is created for one of the following two
/// use cases.
/// 
/// 1. Write-once-then-read-many. In this use case, all the content of a 
/// `CryptoLog` is written in a single run. 
/// Writing in a single run won't trigger any updates to MHT nodes and thus
/// no waste of disk space.
/// After the writing is done, the `CryptoLog` becomes read-only.
/// 
/// 2. Write-many-then-read-once. In this use case, the content of a 
/// `CryptoLog` may be written in many runs. But the number of `CryptoLog`
/// under such workloads is limited and their lengths are also limited.
/// So the disk space wasted by such `CryptoLog` is bounded.
/// And after such `CryptoLog`s are done writing, they will be read once and
/// then discarded. 
pub struct CryptoLog<L> {
    block_log: L,
    key: Key,
    root_meta: Option<RootMhtMeta>,
}

/// The metadata of the root MHT node of a `CryptoLog`.
pub struct RootMhtMeta {
    pub pos: BlockId,
    pub mac: Mac,
    pub iv: Iv,
}

struct MhtNode {
    // The height of the MHT tree whose root is this node
    height: u8,
    // The total number of valid data blocks covered by this node
    num_data_blocks: u32,
    // The child nodes
    children: [NodeMeta; NUM_BRANCHES],
}

struct DataNode([u8; BLOCK_SIZE]);

const NUM_BRANCHES: usize = (BLOCK_SIZE - 8) / mem::size_of::<NodeMata>();

struct NodeMeta {
    pos: BlockId,
    key: Key,
    mac: Mac,
}

// TODO: Need a node cache

impl CryptoLog {
    /// Creates a new `CryptoLog`.
    /// 
    /// A newly-created instance won't occupy any space on the `block_log`
    /// until the first flush, which triggers writing the root MHT node.
    pub fn new(block_log: L) -> Self {
        todo!()
    }

    /// Opens an existing `CryptoLog` backed by a `block_log`.
    /// 
    /// The given key and the metadata of the root MHT are sufficient to
    /// load and verify the root node of the `CryptoLog`.
    pub fn open(block_log: L, key: Key, root_meta: RootMhtMeta) -> Result<()> {
        todo!()
    }

    /// Returns the root key.
    pub fn key(&self) -> &Key {
        &self.key
    }

    /// Returns the metadata of the root block.
    pub fn root_meta(&self) -> Option<&RootMeta> {
        self.root_meta.as_ref()
    }

    /// Returns the number of data blocks.
    pub fn num_blocks(&self) -> usize {
        todo!()
    }

    /// Reads one or multiple data blocks at a specified position.
    pub fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()> {
        todo!()
    }

    /// Appends one or multiple data blocks at the end.
    pub fn append(&mut self, buf: &impl BlockBuf) -> Result<()> {
        todo!()
    }

    /// Ensures that all new data are persisted.
    /// 
    /// Each successful flush triggers writing a new version of the root MHT
    /// node to the underlying block log. The metadata of the latest root MHT
    /// can be obtained via the `root_meta` method.
    pub fn flush(&mut self) -> Result<()> {
        todo!()
    }
}