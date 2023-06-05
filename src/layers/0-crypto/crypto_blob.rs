use crate::bio::BlockSet;

/// A cryptographically-protected blob of user data.
/// 
/// `CryptoBlob<B>` allows a variable-length of user data to be securely
/// written to and read from a fixed, pre-allocated block set
/// (represented by `B: BlockSet`) on disk. Obviously, the length of user data 
/// must be smaller than that of the block set.
/// 
/// # On-disk format
/// 
/// The on-disk format of `CryptoBlob` is shown below.
/// 
/// ```
/// ┌─────────┬─────────┬─────────┬──────────────────────────────┐
/// │VersionId│   MAC   │  Length │       Encrypted Payload      │
/// │  (8B)   │  (16B)  │   (8B)  │        (Length bytes)        │
/// └─────────┴─────────┴─────────┴──────────────────────────────┘
/// ```
/// 
/// The version ID increments by one each time the `CryptoBlob` is updated.
/// The MAC protects the integrity of the length and the encrypted payload.
/// 
/// # Security
/// 
/// To ensure the confidentiality and integrity of user data, `CryptoBlob`
/// takes several meansures:
/// 
/// 1. Each instance of `CryptoBlob` is associated with a randomly-generated,
/// unique encryption key.
/// 2. Each instance of `CryptoBlob` maintains a version ID, which is 
/// automatically incremented by one upon each write.
/// 3. The user data written to a `CryptoBlob` is protected with authenticated
/// encrytion before being persiting to the disk.
/// The encryption takes the current version ID as the IV and generates a MAC 
/// as the output.
/// 4. To read user data from a `CryptoBlob`, it first decrypts 
/// the untrusted on-disk data with the encryption key associated with this object
/// and validating its integrity. Optinally, the user can check the version ID
/// of the decrypted user data and see if the version ID is up-to-date.
/// 
pub struct CryptoBlob<B> {
    block_set: B,
    key: Key,
    curr_version: VersionId,
}

impl<B: BlockSet> CryptoBlob<B> {
    /// Opens an existing `CryptoBlob`.
    /// 
    /// The capacity of this `CryptoBlob` object is determined by the size 
    /// of `block_set: B`.
    pub fn open(key: Key, block_set: B) -> Result<Self> {
        let curr_version = todo!("read from the block set");
        Self {
            block_set,
            key,
            curr_version,
        }
    }

    /// Creates a new `CryptoBlob`.
    /// 
    /// The encryption key of a `CryptoBlob` is generated randomly so that
    /// no two `CrytpBlob` instances shall ever use the same key.
    pub fn create(block_set: B, init_data: &[u8]) -> Result<Self> {
        todo!("write encrypted data to disk");
        Self {
            block_set,
            key,
            curr_version: 0,
        }
    }

    /// Write the buffer to the disk as the latest version of the content of
    /// this `CryptoBlob`.
    /// 
    /// The size of the buffer must not be greater than the capacity of this
    /// `CryptoBlob`.
    /// 
    /// Each successful write increments the version ID by one.
    /// 
    /// # Security
    /// 
    /// This content is guranteed to be confidential as long as the key is not
    /// known to an attacker.
    pub fn write(&mut self, buf: &[u8]) -> Result<VersionId> {
        todo!()
    }

    /// Read the content of the `CryptoBlob` from the disk into the buffer.
    /// 
    /// The given buffer must has a length that is no less than the size of 
    /// the plaintext content of this `CryptoBlob`.
    /// 
    /// # Security
    /// 
    /// This content, including its length, is guranteed to be authentic.
    /// The returned version ID can be checked for freshness.
    pub fn read(&self, buf: &mut [u8]) -> Result<VersionId> {
        todo!()
    }

    /// Returns the key associated with this `CryptoBlob`.
    pub fn key(&self) -> &Key {
        self.key
    }

    /// Returns the current version ID.
    pub fn version_id(&self) -> VersionId {
        self.curr_version
    }

    /// Returns the capacity of this `CryptoBlob` in bytes.
    pub fn capacity(&self) -> usize {
        self.block_set.num_blocks() * BLOCK_SIZE
    }
}
