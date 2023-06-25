use core::mem;
use core::marker::PhantomData;
use postcard::{from_bytes};

use crate::bio::{BlockLog, BlockSet};
use crate::layers::crypto::{CryptoChain, CryptoBlob};
use super::{Edit, EditGroup};

/// The journal of a series of edits to a persistent state.
/// 
/// `EditJournal` is designed to cater the needs of a usage scenario
/// where a persistent state is updated with incremental changes and in high
/// frequency. Apparently, writing the latest value of the
/// state to disk upon every update would result in a poor performance.
/// So instead `EditJournal` keeps a journal of these incremental updates,
/// which are called _edits_. Collectively, these edits can represent the latest
/// value of the state. Edits are persisted in batch, thus the write performance
/// is superior.
/// Behind the scene, `EditJournal` leverages a `CryptoChain` to store the edit 
/// journal securely.
/// 
/// # Compaction
/// 
/// As the total number of edits amounts over time, so does the total size of 
/// the storage space consumed by the edit journal. To keep the storage
/// consumption at bay, accumulated edits are merged into one snapshot periodically,
/// This process is called compaction.
/// The snapshot is stored in a location independent from the journal,
/// using `CryptoBlob` for security. The MAC of the snapshot is stored in the
/// journal. Each `EditJournal` keeps two copies of the snapshots so that even
/// one of them is corrupted due to unexpected crashes, the other is still valid.
/// 
/// # Atomicity
/// 
/// Edits are added to an edit journal individually with the `add` method but
/// are committed to the journal atomically via the `commit` method. This is
/// done by buffering newly-added edits into an edit group, which is called
/// the _current edit group_. Upon commit, the current edit group is persisted
/// to disk as a whole. It is guaranteed that the recovery process shall never
/// recover a partial edit group.
pub struct EditJournal<S /* State */, D /* BlockSet */, P /* Policy */> {
    state: S,
    journal_chain: CryptoChain<BlockRing<D>>,
    snapshot_blobs: [CryptoBlob<D>; 2],
    compaction_policy: P,
    curr_edit_group: EditGroup<S>,
    write_buf: WriteBuf,
}

/// The metadata of an edit journal.
/// 
/// The metadata is mainly useful when recovering an edit journal after a reboot.
pub struct EditJournalMeta {
    /// The number of blocks reserved for storing a snapshot `CryptoBlob`.
    pub snapshot_area_nblocks: usize,
    /// The key of a snapshot `CryptoBlob`.
    pub snapshot_area_keys: [Key; 2],
    /// The number of blocks reserved for storing the journal `CryptoChain`.
    pub journal_area_nblocks: usize,
    /// The key of the `CryptoChain`.
    pub journal_area_key: Key,
}

impl EditJournalMeta {
    /// Returns the total number of blocks occupied by the edit journal.
    pub fn total_nblocks(&self) -> usize {
        self.snapshot_area_nblocks * 2 + self.journal_area_nblocks
    }
}

impl<S, D, P> EditJournal<S, D, P>
where
    D: BlockSet,
    P: CompactionPolicy,
{
    /// Format the disk for storing an edit journal with the specified
    /// configurations, e.g., the initial state.
    pub fn format(
        disk: D,
        init_state: S,
        state_max_nbytes: usize,
        compaction_policy: P,
    ) -> Result<EditJournal> {
        todo!()
    }

    /// Recover an existing edit journal from the disk with the given
    /// configurations.
    /// 
    /// If the recovery process succeeds, the edit journal is returned
    /// and the state represented by the edit journal can be obtained
    /// via the `state` method. 
    pub fn recover(
        disk: D,
        meta: &EditJournalMeta,
        compaction: CompactionPolicy
    ) -> Result<Self> {
        todo!()
    }

    /// Returns the state represented by the journal.
    pub fn state(&self) -> &S {
        &self.state
    }

    /// Returns the metadata of the edit journal.
    pub fn meta(&self) -> EditJournalMeta {
        EditJournalMeta {
            snapshot_area_nblocks: self.snapshot_blobs[0].nblocks(),
            snapshot_area_keys: [
                self.snapshot_blobs[0].key().to_owned(),
                self.snapshot_blobs[1].key().to_owned(),
            ],
            journal_area_nblocks: self.journal_chain.inner_log().nblocks(),
            journal_area_key: self.journal_chain.key().to_ownwed(),
        }
    }

    /// Add an edit to the current edit group.
    pub fn add<E: Edit<S>>(&mut self, edit: &E) {
        self.curr_edit_group.add(edit);
    }

    /// Commit the current edit group.
    pub fn commit(&mut self) {
        if self.curr_edit_group.is_empty() {
            return;
        }

        self.write(&self.curr_edit_group);
        self.object.apply(&self.curr_edit_group);
        self.compaction_policy.on_commit_edits(&self.curr_edit_group);

        self.curr_edit_group.clear();
    }

    fn write(&mut self, edit_group: &EditGroup<S>) {
        let is_first_try_success = self.write_buf.write(edit_group).is_some();
        if is_first_try_success {
            return;
        }

        // TODO: sync disk first to ensure data are persisted before 
        // journal records.

        let write_data = self.write_buf.as_slice();
        self.journal_chain
            .append(write_data)
            // TODO: how to handle I/O error in journaling?
            .expect("we cannot handle I/O error in journaling gracefully");
        self.write_buf.clear();

        if self.compaction_policy.should_compact() {
            self.compact()?;
            self.compaction_policy.done_compact();
        }

        let is_second_try_success = self.write_buf.write(edit_group).is_some();
        if is_second_try_success {
            panic!("the write buffer must have enough free space");
        }
    }

    /// Abort the current edit group by removing all its contained edits.
    pub fn abort(&mut self) {
        self.curr_edit_group.clear();
    }

    fn compact(&mut self) -> Result<()> {
        todo!()
    }
}

/// A journal record in an edit journal.
#[derive(Serialize, Deserialize)]
enum Record<S> {
    /// A record refers to a state snapshot of a specific MAC.
    Version(Mac),
    /// A record that contains an edit group.
    Edit(EditGroup<S>),
}

/// A buffer for writing journal records into an edit journal.
/// 
/// The capacity of `WriteBuf` is equal to the (available) block size of 
/// `CryptoChain`. Records that are written to an edit journal are first
/// be inserted into the `WriteBuf`. When the `WriteBuf` is full or almost full, 
/// the buffer as a whole will be written to the underlying `CryptoChain`.
struct WriteBuf<S> {
    buf: Box<[u8]>,
    avail_begin: usize,
    avail_end: usize,
    phantom: PhantomData<S>,
}

impl<S> WriteBuf<S> {
    /// Creates a new instance.
    pub fn new() -> Self {
        todo!()
    }

    /// Writes a record into the buffer.
    pub fn write(&mut self, record: &Record<S>) -> Option<()> {
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
struct RecordSlice<'a, S> {
    buf: &'a [u8],
    phantom: PhantomData<S>,
    any_error: bool,
}

impl<'a, S> RecordSlice<'a, S> {
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

impl<'a, S> Iterator for RecordSlice<'a, S> {
    type Item = Record<S>; 

    fn next(&mut self) -> Option<Record<S>> {
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
pub trait CompactPolicy<S> {
    /// Called when an edit group is committed.
    /// 
    /// As more edits are accmulated, the compaction policy is more likely to
    /// decide that now is the time to compact.
    fn on_commit_edits(&mut self, edits: &EditGroup<S>);

    /// Returns whether now is a good timing for compaction.
    fn should_compact(&self) -> bool;

    /// Reset the state, as if no edits have ever been added.
    fn done_compact(&mut self);
}

/// A never-do-compaction policy. Mostly useful for testing.
pub struct NeverCompactPolicy;

impl<S> CompactPolicy<S> for NeverCompactPolicy {
    fn on_commit_edits(&mut self, _edits: &EditGroup<S>) {}

    fn should_compact(&self) -> bool { false }

    fn done_compact(&mut self) {}
}