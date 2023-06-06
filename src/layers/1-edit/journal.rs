use core::mem;
use core::marker::PhantomData;
use postcard::{from_bytes};

use crate::bio::{BlockLog, BlockSet};
use crate::layers::crypto::{CryptoChain, CryptoBlob};
use super::Edit;

/// The journal of a series of edits to an object.
/// 
/// By tracking all the edits made to an object, `EditJournal` maintains
/// the latest state of an object that may be updated with incremental changes
/// and in high frequency. Behind the scene, `EditJournal` leverages 
/// a `CryptoChain` to store the edit sequence securely.
/// 
/// As the total number of edits amounts over time, so does the total size of 
/// the storage space consumed by the edit journal. To keep the storage
/// consumption at bay, object edits are merged into one object snapshot periodically.
/// This process is called compaction.
/// The snapshot is stored in a location independent from the journal,
/// using `CryptoBlob` for security. The MAC of the snapshot is stored in the
/// journal. Each `EditJournal` keeps two copies of the snapshots so that even
/// one of them is corrupted due to unexpected crashes, the other is still valid.
pub struct EditJournal<E: Edit, L, S, P> {
    journal_chain: CryptoChain<L>,
    snapshot_blobs: [CryptoBlob<S>; 2],
    compaction_policy: P,
    object: E::Object,
    write_buf: WriteBuf,
}

impl<E, L, S, P> EditJournal<E, L, S, P> 
where
    E: Edit,
    L: BlockLog,
    S: BlockSet,
    P: CompactPolicy<E>,
{
    /// Format the given `block_log` and `block_sets` as the storage for a
    /// new `EditJournal`, returning the crypo-protected versions of the 
    /// storage.
    /// 
    /// After formatting, the crypto-protected versions of storage will be 
    /// returned. Later, they can be feed to the `recover` method
    /// to open the `EditJournal`. The initial state of the object represented
    /// by the journal is as specified by `init_object`.
    pub fn format(
        init_object: E::Object,
        block_log: L,
        block_sets: [S; 2]
    ) -> Result<(CryptoChain<L>, CryptoBlob<S>)> {
        todo!()
    }

    /// Recover the state of an `EditJournal` given its crypo-protected storage
    /// regions.
    /// 
    /// # Panics
    /// 
    /// The user must make sure that the given storage indeed stores an 
    /// `EditJournal` for edit type `E`. Otherwise, attempts to deserilize edits
    /// will fail, causing panics.
    pub fn recover(
        journal_chain: CryptoChain<L>,
        version_blobs: [CryptoBlob<S>; 2],
        compaction_policy: CompactPolicy,
    ) -> Result<Self> {
        todo!()
    }

    /// Returns the object represented by the journal.
    pub fn object(&self) -> &E::Object {
        &self.object
    }

    /// Apply an edit to the object represented by the edit journal and write
    /// a journal record to persist the edit.
    pub fn apply(&mut self, edit: &E) -> Result<()> {
        edit.apply_to(&mut self.object)?;

        self.write(edit)
            .expect("edit has been applied the object; no way to rollback");

        self.compaction_policy.accmulate_edit(edit);

        Ok(())
    }

    fn write(&mut self, edit: &E) -> Result<()> {
        let is_first_try_success = self.write_buf.write(edit).is_some();
        if is_first_try_success {
            return Ok(());
        }

        let write_data = self.write_buf.as_slice();
        self.journal_chain.append(write_data)?;
        self.write_buf.clear();

        let is_second_try_success = self.write_buf.write(edit).is_some();
        if is_second_try_success {
            panic!("the write buffer must have enough free space");
        }
        Ok(())
    }

    /// Ensure all edits are persisted.
    pub fn flush(&mut self) -> Result<()> {
        if self.compaction_policy.should_compact() {
            self.compact()?;
            self.compaction_policy.reset();
        }

        self.journal_chain.flush()?;
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        todo!()
    }
}

/// A journal record in an edit journal.
#[derive(Serialize, Deserialize)]
enum Record<E> {
    /// A record refers to an edit version of a specific MAC.
    Version(Mac),
    /// A record that contains an edit.
    Edit(E),
}

/// A buffer for writing records into an edit journal.
/// 
/// The capacity of `WriteBuf` is equal to the (available) block size of 
/// `CryptoChain`. Records that are written to an edit journal will first
/// be inserted into the `WriteBuf`. When the `WriteBuf` is (near) full, 
/// the buffer as a whole will be written to the underlying `CryptoChain`.
struct WriteBuf<E> {
    buf: Box<[u8]>,
    avail_begin: usize,
    avail_end: usize,
    phantom: PhantomData<E>,
}

impl<E: Edit> WriteBuf<E> {
    /// Creates a new instance.
    pub fn new() -> Self {
        todo!()
    }

    /// Writes a record into the buffer.
    pub fn write(&mut self, record: &Record<E>) -> Option<()> {
        // Write the record at the beginning of the avail buffer
        let serial_record_len = match postcard::to_slice(record, self.avail_buf()) {
            Ok(serial_record) => serial_record.len(),
            Err(e) => {
                if e != SerializeBufferFull {
                    panic!("Errors (except SerializeBufferFull) are not expected");
                }
                return None
            },
        };
        self.avail_begin += serial_record_len;

        // Write the length of the serialized record at the end of the avail buffer
        let serial_record_len_bytes = serial_record_len::to_be_bytes();
        let avail_buf = self.avail_buf();
        avail_buf[avail_buf.len() - U16_LEN..avail_buf.len()]
            .copy_from_slice(serial_record_len_bytes);
        self.avail_end -= U16_LEN;

        debug_assert!(self.avail_begin <= self.avail_end);
        
        Some(())
    }

    /// Clear all records in the buffer.
    pub fn clear(&mut self) {
        self.avail_begin = 0;
        self.avail_end = self.buf.len();
    }

    /// Returns a slice containing the data in the write buffer.
    pub fn as_slice(&self) -> &[u8] {
        // It is important to fill zeros in the available buffer for two reasons.
        // First, the array of record lengths must be terminated with a zero.
        // Second, we do not want to leave part of the write buffer uninitialized.
        //
        // Note that this method is the only way to access the internal raw data.
        // So delaying zeroing to this point in this method is safe.
        debug_assert!(self.avail_len() >= U16_LEN);
        self.avail_buf().fill(0);

        &*self.buf
    }

    fn avail_len(&self) -> usize {
        self.avail_end - self.avail_begin
    }

    fn avail_buf(&self) -> &mut [u8] {
        &mut self.buf[self.avail_begin..self.avail_end]
    }
}

/// A byte slice containing serialized edit records. 
/// 
/// The slice allows deserializing and iterates the contained edit records.
struct RecordSlice<'a, E> {
    buf: &'a [u8],
    phantom: PhantomData<E>,
    any_error: bool,
}

impl<'a, E> RecordSlice<'a, E> {
    /// Create a new slice of edit records in serialized form.
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            buf,
            phantom: PhantomData,
            any_error: false,
        }
    }

    /// Returns if any error occurs while deserializing the records.
    pub fn any_error(&self) -> bool {
        self.any_error
    }
}

impl<'a, E: Edit> Iterator for RecordSlice<'a, E> {
    type Item = Record; 

    fn next(&mut self) -> Option<Record<E>> {
        let record_len = {
            if self.buf.len() <= U16_LEN {
                return None;
            }

            let record_len_bytes = self.buf[self.buf.len() - U16_LEN..self.buf.len()]
                .try_into()
                .unwrap(); // the length of the slice and array are equal
            u16::from_be_bytes(record_len_bytes)
        };
        // A record length of value 0 marks the end
        if record_len == 0 {
            return None;
        }

        let record_bytes = {
            if self.buf.len() < U16_LEN + record_len {
                return None;
            }
            self.buf[..record_len]
        };

        let Ok(record) = postcard::from_bytes(record_bytes) else {
            self.any_error = true;
            return None;
        };

        self.buf = &self.buf[record_len..self.buf.len() - U16_LEN];
        Some(record)
    }
}

const U16_LEN: usize = mem::size_of::<u16>();

/// A compaction policy, which decides when is the good timing for compacting
/// the edits in an edit journal.
pub trait CompactPolicy<E: Edit> {
    /// Accmulate one more edit.
    /// 
    /// As more edits are accmulated, the compact policy is more likely to
    /// decide that it is now a good time to compact.
    fn accumulate(&mut self, edit: &E);

    /// Returns whether now is a good timing for compaction.
    fn should_compact(&self) -> bool;

    /// Reset the state, as if no edits have ever been added.
    fn reset(&mut self);
}