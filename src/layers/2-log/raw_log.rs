/// A store of raw (untrusted) logs.
/// 
/// `RawLogStore<D>` allows creating, deleting, reading and writing 
/// `RawLog<D>`. Each raw log is uniquely identified by its ID (`RawLogId`).
/// Writing to a raw log is append only.
/// 
/// `RawLogStore<D>` stores raw logs on a disk of `D: BlockSet`.
/// Internally, `RawLogStore<D>` manages the disk space with `ChunkAlloc`
/// so that the disk space can be allocated and deallocated in the units of
/// chunk. An allocated chunk belongs to exactly one raw log. And one raw log 
/// may be backed by multiple chunks.
/// 
/// # Examples
/// 
/// Raw logs are manipulated and accessed within transactions.
/// 
/// ```
/// fn concat_logs<S>(
///     log_store: &RawLogStore<S>,
///     log_ids: &[RawLogId]
/// ) -> Result<RawLogId> {
///     let mut tx = log_store.new_tx();
///     let res = tx.context(|| {
///         let mut buffer = BlockBuf::new_boxed();
///         let output_log = log_store.create_log();
///         for log_id in log_ids {
///             let input_log = log_store.open_log(log_id, false)?;
///             let input_len = input_log.num_blocks();
///             let mut pos = 0;
///             while pos < input_len {
///                 let read_len = buffer.len().min(input_len - pos);
///                 input_log.read(&mut buffer[..read_len])?;
///                 output_log.write(&buffer[..read_len])?;
///             }
///         }
///         Ok(output_log.id())     
///     });
///     if res.is_ok() {
///         tx.commit()?;
///     } else {
///         tx.abort();
///     }
///     res
/// }
/// ``` 
/// 
/// If any error occurs (e.g., failures to open, read, or write a log) during 
/// the transaction, then all prior changes to raw logs shall have no
/// effects. On the other hand, if the commit operation succeeds, then 
/// all changes made in the transaction shall take effect as a whole.
/// 
/// # Expected behaviors
/// 
/// We provide detailed descriptions about the expected behaviors of raw log 
/// APIs under transactions.
/// 
/// 1. The local changes made (e.g., creations, deletions, writes) in a Tx are 
/// immediately visible to the Tx, but not other Tx until the Tx is committed.
/// For example, a newly-created log within Tx A is immediately usable within Tx,
/// but becomes visible to other Tx only until A is committed.
/// As another example, when a log is deleted within a Tx, then the Tx can no 
/// longer open the log. But other concurrent Tx can still open the log.
///
/// 2. If a Tx is aborted, then all the local changes made in the TX will be 
/// discarded.
///
/// 3. At any given time, a log can have at most one writer Tx.
/// A Tx becomes the writer of a log when the log is opened with the write 
/// permission in the Tx. And it stops being the writer Tx of the log only when 
/// the Tx is terminated (not when the log is closed within Tx).
/// This single-writer rule avoids potential conflicts between concurrent 
/// writing to the same log.
///
/// 4. Log creation does not conflict with log deleation, read, or write as 
/// every newly-created log is assigned a unique ID automatically.
///
/// 4. Deleting a log does not affect any opened instance of the log in the Tx
/// or other Tx (similar to deleting a file in a UNIX-style system). 
/// It is only until the deleting Tx is committed and the last 
/// instance of the log is closed shall the log be deleted and its disk space
/// be freed.
///
/// 5. The Tx commitment will not fail due to conflicts between concurrent 
/// operations in different Tx.
pub struct RawLogStore<D> {
    state: Arc<Mutex<State>>,
    disk: D,
    chunk_alloc: ChunkAlloc,
    tx_provider: Arc<TxProvider>,
}

pub type RawLogId = u64;

//---------------------------------------------------------------------------
// The code below is still in early draft!!!!!
//---------------------------------------------------------------------------

// TODO: Allocate Ids only when the raw logs are commited
// TODO: check log.tx_id == current.tx_id
// TODO: rename persistent state to state
// TODO: rename Change to Edit
// TODO: how to delete
// TODO: Rename <S> to <D>


/// The volatile and persistent state of a `RawLogStore`.
struct State {
    persistent: RawLogStoreState,
    write_set: BTreeSet<RawLogId>,
    next_free_log_id: RawLogId,
}

/// The persistent state of a `RawLogStore`.
pub(super) struct RawLogStoreState {
    log_table: BTreeMap<LogId, Arc<Mutex<RawLogEntry>>>,
    next_free_log_id: RawLogId,
}

struct RawLogEntry {
    head: RawLogHead,
    is_deleted: bool,
}

struct RawLogHead {
    chunks: Vec<ChunkId>,
    num_blocks: usize,
}

struct PersistentState {
}

struct PersistentChange {
    change_table: BTreeMap<LogId, RawLogChange>,
}

enum RawLogChange {
    Create(RawLogCreate),
    Append(RawLogAppend),
    Delete,
}

struct RawLogCreate {
    tail: RawLogTail,
    // Whether the log is deleted after being created in a TX.
    is_deleted: bool,
}

struct RawLogAppend {
    tail: RawLogTail,
    // Whether the log is deleted after being appended in a TX.
    is_deleted: bool,
}

struct RawLogTail {
    // The last chunk of the head. If it is partially filled 
    // (head_last_chunk_free_blocks > 0), then the tail should write to the 
    // free blocks in the last chunk of the head.
    head_last_chunk_id: ChunkId,
    head_last_chunk_free_blocks: u16, 
    // The chunks allocated and owned by the tail
    chunks: Vec<ChunkId>,
    // The total number of blocks in the tail, including the blocks written to
    // the last chunk of head and those written to the chunks owned by the tail.
    num_blocks: usize,
}

impl RawLogHead {
    pub fn append(&mut self, tail: &mut RawLogTail) {
        todo!()
    }
}

impl State {
    pub fn new(persistent: PersistentState) -> Self {
        let next_log_id = persistent.max_log_id.map_or(0, |max| max + 1);
        Self {
            persistent,
            write_set: BTreeSet::new(),
            next_log_id,
        }
    }

    pub fn alloc_log_id(&mut self) -> RawLogId {
        let new_log_id = self.next_log_id;
        self.next_log_id = self.next_log_id
            .checked_add(1)
            .expect("64-bit IDs won't be exhausted even though IDs are not recycled");
        new_log_id
    }

    pub fn add_to_write_set(&mut self, log_id: RawLogId) -> Result<()> {
        let already_exists = self.write_set.insert(log_id);
        if already_exists {
            return Err(Error::AlreadyExists);
        }
        Ok()
    }

    pub fn remove_from_write_set(&mut self, log_id: RawLogId) {
        let is_removed = self.write_set.remove(&log_id);
        debug_assert!(is_removed == false);
    }

    pub fn find_log(&self, log_id: RawLogId) -> Option<Arc<Mutex<RawLogEntry>>> {
        let log_table = &self.persistent.log_table;
        log_table.get(&log_id).map(|entry| entry.clone())
    }

    pub fn commit(&mut self, change: &mut PersistentChange, chunk_alloc: &ChunkAlloc) {
        let mut all_changes = change.change_table.drain_filter(|| true);
        for (log_id, change) in all_changes {
            match change {
                RawLogChange::Create(create) => {
                    let RawLogCreate { tail, is_deleted } = create;
                    if is_deleted {
                        chunk_alloc.dealloc_batch(tail.chunks.iter());
                        continue;
                    }

                    self.create_log(log_id);
                    self.append_log(log_id, &mut tail);
                }
                RawLogChange::Append(append) => {
                    let RawLogAppend { tail, is_deleted } = append;

                    // Even if a log is to be deleted, we will still commit
                    // its newly-appended data.

                    self.append_log(log_id, &mut tail);
                }
                RawLogChange::Delete => {
                    self.delete_log(log_id);
                }
            }
        }
    }

    fn create_log(&mut self, new_log_id: RawLogId) {
        let log_table = &mut self.persistent.log_table;
        let new_log_entry = Arc::new(Mutex::new(RawLogEntry::new()));
        let already_exists = log_table.insert(new_log_id, new_log_entry);
        debug_assert!(already_exists == false);
    }

    fn append_log(&mut self, log_id: RawLogId, tail: &RawLogTail) {
        let log_table = &mut self.persistent.log_table;
        let mut log_entry = log_table.get(&log_id).unwrap().lock();
        log_entry.head.append(tail);
    }

    fn delete_log(&mut self, log_id: RawLogId) {
        let log_table = &mut self.persistent.log_table;
        let Some(log_entry) = log_table.remove(&log_id) else {
            return;
        };

        chunk_alloc.dealloc_batch(log_entry.head.chunks.iter());
    }
}
/*
struct PersistentChange {
    change_table: BTreeMap<LogId, RawLogChange>,
}
 */
impl PersistentChange {
    pub fn create_log(&mut self, new_log_id: RawLogId) {
        let new_log_chanage = RawLogChange::Create(RawLogCreate::new());
        let existing_change = self.change_table.insert(new_log_id, new_log_change);
        debug_assert!(existing_change.is_none());
    }

    pub fn open_log(&mut self, log_id: RawLogId, log_entry: &LogEntry) -> Result<()> {
        match self.change_table.get(&log_id) {
            None => {
                // insert an Append
            }
            Some(change) => {
                // if change == create, no nothing
                // if change == append, no nothing
                // if change == delete, panic
            }
        }
        let new_log_chanage = RawLogChange::Create(RawLogCreate::new());
        self.changes.insert(new_log_id, new_log_change);
    }

    pub fn delete_log(&mut self, log_id: RawLogId) -> Option<_> {
        match self.changes.get(&log_id) {
            None => {
                self.changes.insert(log_id, LogMetaChange::Delete);
            }
            Some(Create(_)) => {
                let create = self.changes.insert(log_id, LogMetaChange::Delete);
                let LogMetaChange::Create(parts) = create else {
                    panic!("");
                };
                self.free_parts(parts);
            }
            Some(Append(_)) => {
                let append = self.changes.insert(log_id, LogMetaChange::Delete);
                let LogMetaChange::Append(parts) = append else {
                    panic!("");
                };
                self.free_parts(parts);
            }
            Some(Delete) => {
                return None;
            }
        }
    }
}



impl<S> RawLogStore<S> {
    pub fn new(
        disk: S,
        tx_manager: Arc<TxManager>,
    ) -> Self {
        todo!()
    }
}

impl<S> RawLogStoreInner<S> {
    pub fn create_log(&self) -> Result<RawLog<S>> {
        let mut state = self.state.lock();
        let mut current_tx = self.tx_provider.current();

        let tx_id = current_tx.id();
        let new_log_id = state.alloc_log_id();
        current_tx.data_mut_with(|change: &mut PersistentChange| {
            change.create_log(new_log_id);
        });

        Ok(RawLog {
            log_id: new_log_id,
            tx_id,
            log_store: self.weak_self.upgrade(),
            log_entry: None,
            can_write: false,
        })
    }

    pub fn open_log(&self, log_id: RawLogId, can_write: bool) -> Result<RawLog<S>> {
        let mut state = self.state.lock();
        let mut current_tx = self.tx_provider.current();

        let log_entry = state.find_log(log_id);
        // The log is already created by other Tx
        if log_entry.is_some() {
            // Prevent other TX from opening this log in the write mode.
            if can_write {
                state.add_to_write_set(log_id)?;
            }
        } else {
            // The log must has been created by this Tx
            let not_created = current_tx.data_mut_with(|change: &mut PersistentChange| {
                change.is_log_created(log_id)
            });
            if !not_created {
                return Err(Error::NotFound);
            }
        }

        // If the log is open in the write mode, change must be prepared
        if can_write {
            current_tx.data_mut_with(|change: &mut PersistentChange| {
                change.open_log(log_id, &*log_entry.lock());
            });
        }
        let tx_id = current_tx.id();

        Ok(RawLog {
            log_id,
            tx_id,
            log_store: self.weak_self.upgrade(),
            log_entry,
            can_write,
        })
    }

    pub fn delete_log(&self, log_id: RawLogId) -> Result<()> {
        let mut current_tx = self.tx_provider.current();
        current_tx.data_mut_with(|change: &mut PersistentChange| {
            change.delete_log(log_id);
        })
    }
}

struct RawLog<S> {
    log_id: RawLogId,
    tx_id: TxId,
    log_store: Arc<RawLogStoreInner<S>>,
    log_entry: Option<Arc<Mutex<RawLogEntry>>>,
    can_write: bool,
}

impl Drop for RawLog {
    fn drop(&mut self) {
        if self.can_write() {
            let mut state = self.log_store.state.lock();
            state.remove_from_write_set(self.log_id);
        }
    }
}

impl<S: BlockSet> BlockLog for RawLog<S> {
    fn read(&self, mut pos: BlockId, mut buf: &mut impl BlockBuf) -> Result<()> {
        debug_assert!(buf.len() > 0);

        let head_opt = self.head();
        let tail_opt = self.tail();
        let head_len = head_opt.map_or(0, |head| head.len());
        let tail_len = tail_opt.map_or(0, |tail| tail.len());
        let total_len = head_len + tail_len;

        // Do not allow "short read"
        if pos + buf.len() > total_len {
            return Err(EINVAL);
        }

        // Read from the head if possible and necessary
        if let Some(head) = head_opt && pos < head_len {
            let read_len = buf.len().min(head_len - pos);
            head.read(pos, &mut buf[..read_len])?;

            pos += read_len;
            buf = &mut buf[read_len..];
        }
        // Read from the tail if possible and necessary
        if let Some(tail) = tail_opt && pos >= head_len {
            let read_len = buf.len().min(total_len - pos);
            tail.read(pos, &mut buf[..read_len])?;
        }
        Ok(())
    }

    fn append(&self, buf: &impl BlockBuf) -> Result<BlockId> {
        debug_assert!(buf.len() > 0);

        if !self.can_write {
            return Err(EPERM);
        }

        self.tail().unwrap().append(buf)
    }

    fn flush(&self) -> Result<()> {
        self.log_store.disk.flush()
    }

    fn num_blocks(&self) -> usize {
        let head_opt = self.head();
        let tail_opt = self.tail();
        let head_len = head_opt.map_or(0, |head| head.len());
        let tail_len = tail_opt.map_or(0, |tail| tail.len());
        head_len + tail_len;
    }

    fn head(&self) -> Option<RawLogHeadRef<'_, S>> {
        
    }

    fn tail(&self) -> Option<RawLogTailRef<'_, S>> {
        todo!()
    }
}

struct RawLogHeadRef<'a, S> {
    log_store: &'a RawLogStoreInner<S>,
    log_entry: MutexGuard<'a, RawLogEntry>,
}

struct RawLogTailRef<'a, S> {
    log_store: &'a RawLogStoreInner<S>,
    log_tail: &'a mut RawLogTail,
}

struct RawLogRef<'a, S> {
    log_store: &'a RawLogStoreInner<S>,
    log_entry: MutexGuard<'a, RawLogEntry>,
    log_tail: &'a mut RawLogTail,
}

impl<'a, S> RawLogRef<'a, S> {
    fn read(&self, mut log_pos: BlockId, mut buf: &mut impl BlockBuf) -> Result<()> {

    }

    fn append(&mut self, buf: &impl BlockBuf) -> Result<()> {
    }

    pub fn len(&self) -> usize {
        self.log_entry.num_blocks + self.log_tail.len()
    }

    /// Get a number of consecutive blocks starting at a specified position.
    /// 
    /// The start of the returned range is always equal to `pos`.
    /// The length of the returned range is not greater than `max_len`.
    /// 
    /// If `pos` is smaller than the length of the raw log,
    /// then the returned range is non-empty. Otherwise, the returned range
    /// is empty.
    fn get_consecutive(&self, pos: BlockId, max_len: usize) -> Range<BlockId> {
        todo!()
    }

    /// Extend the length of `ChunkBackedBlockVec` by allocating some 
    /// consecutive blocks.
    /// 
    /// The newly-allocated consecutive blocks are returned as a block range,
    /// whose length is not greater than `max_len`. If the allocation fails,
    /// then a zero-length range shall be returned.
    fn extend_consecutive(&mut self, max_len: usize) -> Option<Range<BlockId>> {
        todo!()
    }
}


impl<S> BlockLog for RawLog<S> {
    fn read_len(&self) -> usize {

    }
}

struct PersistentState {
    chunk_logs: BTreeMap<RawLogId, RawLogState>, 
    max_log_id: Option<u64>,
}

struct PersistentChange {
    changed_logs: BTreeMap<ChunkLogId, ChunkLogState>, 
    max_log_id: Option<u64>,
}

struct RawLogState {
    blocks: Arc<ChunkBackedBlockVec>,
    len: usize,
    is_deleted: bool,
}

// What's the expected behaviors of create, open, delete, read, write of
// raw logs in Tx?
//
// 1. The local changes made (e.g., creations, deletions, writes) in a Tx are 
// immediately visible to the Tx, but not other Tx until the Tx is committed.
// For example, a newly-created log within Tx A is immediately usable within Tx,
// but becomes visible to other Tx only until A is committed.
// As another example, when a log is deleted within a Tx, then the Tx can no 
// longer open the log. But other concurrent Tx can still open the log.
//
// 2. If a Tx is aborted, then all the local changes made in the TX will be 
// discarded.
//
// 3. At any given time, a log can have at most one writer Tx.
// A Tx becomes the writer of a log when the log is opened with the write 
// permission in the Tx. And it stops being the writer Tx of the log only when 
// the Tx is terminated (not when the log is closed within Tx).
// This single-writer rule avoids potential conflicts between concurrent 
// writing to the same log.
//
// 4. Log creation does not conflict with log deleation, read, or write as 
// every newly-created log is assigned a unique ID automatically.
//
// 4. Deleting a log does not affect any opened instance of the log in the Tx
// or other Tx (similar to deleting a file in a UNIX-style system). 
// It is only until the deleting Tx is committed and the last 
// instance of the log is closed shall the log be deleted and its disk space
// be freed.
//
// 5. The Tx commitment will not fail due to conflicts between concurrent 
// operations in different Tx.

// How to create?
//
// 1. 

// * State and Change are quite similar.
// * Change must be self-contained.

struct RawLog {
    meta: Arc<Mutex<LogMeta>>,
}

impl Drop for RawLog {
    fn drop(&mut self) {
        let mut meta = self.meta.lock();
        meta.delete_count
    }
}




impl PersistentState {
    pub fn commit(&mut self, change: &mut PersistentChange) {

    }

    pub fn uncommit(&mut self, change: &mut PersistentChange) {

    }
}

struct RawLog {
    id: RawLogId,

}



// How to delete?
// 1. Mark the log deleted in Change. so that the log
// cannot be opened again within the transaction.
// 2. When commit, mark the log deleted in State. This way,
// other transactions that are still using the log can continue
// use the log. But future transactions are not allowed to open
// the deleted log.
// 3. When the last active instance of the deleted log (which 
// could be in Change or State) is dropped
// (wh),
// then 

// A log joins a Tx when it is opened. And leaves the Tx
// when the Tx ends.
// If the log joins the Tx with writable mode, then
// no other Tx can open it.