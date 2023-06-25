
use crate::layers::crypto::RootMhtMeta;
use crate::layers::edit::{Edit, EditJournal};

use self::journaling::{Journal, JournalCompactPolicy};

/// A store of transactional logs.
pub struct TxLogStore<D> {
    state: Arc<Mutex<State>>,
    key: Key,
    raw_log_store: Arc<RawLogStore<D>>,
    journal: Arc<Journal<D>>,
    tx_provider: Arc<TxProvider>,
}

pub type TxLogId = RawLogId;

struct Superblock {
    journal_area_meta: EditJournalMeta,
    chunk_area_nblocks: usize,
}

impl Superblock {
    /// Returns the total number of blocks occupied by the `TxLogStore`.
    pub fn total_nblocks(&self) -> usize {
        self.journal_area_meta.total_nblocks() + self.chunk_area_nblocks
    }
}

/// The volatile and persistent state of a `TxLogStore`.
struct State {
    persistent: TxLogStoreState,
    log_table: BTreeMap<TxLogId, Arc<LazyDelete<RawLogId>>>,
}

/// The persistent state of a `TxLogStore`.
struct TxLogStoreState {
    log_table: BTreeMap<TxLogId, TxLogEntry>,
    bucket_table: BTreeMap<BucketName, Bucket>,
}

struct TxLogEntry {
    bucket: BucketName,
    key: Key,
    root_mht: RootMhtMeta,
}

type BucketName = Arc<String>;

struct Bucket {
    log_ids: BTreeSet<TxLogId>,
}

struct TxLogStoreEdit {
    edit_table: BTreeMap<TxLogId, TxLogEdit>,
}

enum TxLogEdit {
    Create(TxLogCreate),
    Append(TxLogAppend),
    Delete,
}

struct TxLogCreate {
    bucket: BucketName,
    key: Key,
    root_mht: Option<RootMhtMeta>,
}

struct TxLogAppend {
    root_mht: RootMhtMeta,
}

struct OpenLogTable<D> {
    log_table: BTreeMap<TxLogId, Arc<TxLogInner<D>>>,
}

impl State {
    pub fn new(
        persistent: TxLogStoreState,
        raw_log_store: &Arc<RawLogStore<D>>,
    ) -> Self {
        todo!("init LazyDelete")
    }

    pub fn apply(&mut self, edit: &TxLogStoreEdit) {
        edit.apply_to(&mut self.persistent); 

        // Do lazy deletion of logs
        let deleted_log_ids = edit.edit_table.iter_deleted_logs();
        for deleted_log_id in deleted_log_ids {
            let Some(lazy_delete) = state.log_table.remove(delete_log_id) else {
                // Other concurrent TXs have deleted the same log
                continue;
            };
            LazyDelete::delete(&lazy_delete);
        }
    }
}

impl Edit<TxLogStoreState> for TxLogStoreEdit {
    fn apply_to(&self, state: &mut TxLogStoreState) {
        for (log_id, log_edit) in &self.edit_table {
            match log_edit {
                TxLogEdit::Create(create_log) => {
                    let TxLogCreate { bucket, key, root_mht, .. } = create_log;
                    state.create_log(log_id, bucket, key, root_mht.unwrap());
                }
                TxLogEdit::Append(append_log) => {
                    let TxLogAppend { root_mht, .. } = append_log;
                    state.append_log(log_id, root_mht);
                }
                TxLogEdit::Delete => {
                    state.delete_log(log_id);
                }
            }
        }
    }
}

impl TxLogStoreState {
    pub fn create_log(
        &mut self,
        new_log_id: TxLogId,
        bucket: BucketName,
        key: Key,
        root_mht: RootMhtMeta,
    ) {
        todo!()
    }

    pub fn append_log(&mut self, root_mht: RootMhtMeta) {
        todo!()
    }

    pub fn list_logs(&self, bucket_name: &str) -> Result<BTreeSet<TxLogId>> {
        let bucket = self.bucket_table.get(bucket_name).ok_or(Error::NotFound)?;
        Ok(bucket.log_ids.clone());
    }

    pub fn find_log(&self, log_id: TxLogId) -> Result<&TxLogEntry> {
        self.log_table.get(&log_id).ok_or(Error::NotFound)
    }

    pub fn contains_log(&self, log_id: TxLogId) -> bool {
        self.log_table.contains(&log_id)
    }

    pub fn delete_log(&mut self, log_id: TxLogId) {
        // Do not check the result because concurrent TXs may decide to delete
        // the same logs.
        let _ = self.log_table.remove(&log_id);
    }
}

impl TxLogStoreEdit {
    pub fn is_log_deleted(&self, log_id: TxLogId) -> bool {
        todo!()
    }

    pub fn is_log_created(&self, log_id: TxLogId) -> bool {
        todo!()
    }

    pub fn delete_log(&mut self, log_id: TxLogId) -> Result<()> {
        match self.edit_table.get_mut(&log_id) {
            None => {
                self.edit_table.insert(log_id, TxLogEdit::Delete);
            }
            Some(TxLogEdit::Create(_)) => {
                self.edit_table.remove(log_id);
            }
            Some(TxLogEdit::Append(_)) => {
                panic!("TxLogEdit::Append is added at very late stage, after which logs won't get deleted");
            }
            Some(TxLogEdit::Delete) => {
                return Err(Error::NotFound);
            }
        }
        Ok(())
    }

    pub fn iter_deleted_logs(&self) -> impl Iterator<Item = LogId> {
        self.edit_table.iter
    }

    pub fn update_log_metas<I>(&mut self, iter: I)
    where
        I: Iterator<Item = (TxLogId, RootMhtMeta)>, 
    {
        // For newly-created logs, update RootMhtMeta
        // For existing logs that are appended, add TxLogAppend
        todo!()
    }
}

impl<D: BlockSet> TxLogStore<D> {
    /// Format the disk to create a new instance of `TxLogStore`. 
    /// 
    /// Each instance will be assigned a unique, automatically-generated root 
    /// key.
    pub fn format(disk: D) -> Result<Self> {
        todo!()
    }

    /// Recover an existing `TxLogStore` from a disk using the given key.
    pub fn recover(disk: D, key: Key) -> Result<Self> {
        let sb = SuperBlock::open(disk.subset(0..1), key)?;

        if storage.nblocks() < sb.required_nblocks() {
            return Err(Error::NotEnoughSpace);
        }
        
        let tx_provider = TxProvider::new();

        let journal = {
            let journal_area_size = sb.journal_area_meta.total_nblocks();
            let journal_area = disk.subset(1..1 + journal_area_size);
            let journal = Journal::recover(
                journal_area,
                &sb.journal_area_meta,
                JournalCompactPolicy::new(),
            )?;
            let journal = Arc::new(Mutex::new(journal));

            tx_provider.register_commit_handler({
                let journal = journal.clone();
                |current| {
                    let mut journal = journal.lock();
                    current.data_with(|edit: &ChunkAllocEdit| {
                        journal.add(edit);
                    });
                    current.data_with(|edit: &RawLogStoreEdit| {
                        journal.add(edit);
                    });
                    current.data_with(|edit: &TxLogStoreEdit| {
                        journal.add(edit);
                    });
                    journal.commit();
                }
            });

            journal
        };

        let state = journal.state();
        let chunk_alloc = ChunkAlloc::from_parts(
            state.chunk_alloc.clone(), tx_provider.clone());
        let chunk_area = disk.subset(1 + journal_area_size..
            1 + journal_area_size + sb.chunk_area_nblocks);
        let raw_log_store = RawLogStore::from_parts(
            state.raw_log_store.clone(), chunk_area, chunk_alloc, tx_provider.clone()
        );
        let tx_log_store = TxLogStore::from_parts(
            state.tx_log_store.clone(), key, raw_log_store, journal, tx_provider
        );
        Ok(tx_log_store)
    }

    fn from_parts(
        state: TxLogStoreState,
        key: Key,
        raw_log_store: RawLogStore<D>,
        journal: Journal<D>,
        tx_provider: Arc<TxProvider>,
    ) -> Self {
        let new_self = Self {
            state: Arc::new(Mutex::new(state)),
            key,
            raw_log_store,
            journal,
            tx_provider,
        };

        tx_provider.register_data_initializer(|| {
            TxLogStoreEdit::new()
        });
        tx_provider.register_data_initializer(|| {
            OpenLogTable::new()
        });
        tx_provider.register_precommit_handler({
            |current| {
                // Do I/O in the pre-commit phase. If any I/O error occured,
                // the TX would be aborted.
                let dirty_log_ids_and_metas = current.data_with(|open_log_table: &OpenLogTable<D>| {
                    let mut ids_and_metas = Vec::new();
                    for (log_id, inner_log) in &open_log_table.log_table {
                        if !inner_log.is_dirty {
                            continue;
                        }

                        let crypto_log = &log_inner.crypto_log;
                        crypto_log.flush()?;
                        let new_log_meta = *crypto_log.root_meta().unwrap();
                        ids_and_metas.push((log_id, new_log_meta));
                    }
                    Ok(ids_and_metas)
                })?;
                current.data_mut_with(|store_edit: &mut TxLogStoreEdit| {
                    store_edit.update_log_metas(dirty_log_id_meta_pairs.iter())
                });
                Ok(())
            }
        });
        tx_provider.register_commit_handler({
            let state = new_self.state.clone();
            |current| {
                current.data_with(|store_edit: &TxLogStoreEdit| {
                    let mut state = state.lock();
                    state.apply(&store_edit);
                });
            }
        });

        new_self
    }

    /// List the IDs of all logs in a bucket.
    /// 
    /// # Panics 
    ///
    /// This method must be called within a TX. Otherwise, this method panics. 
    pub fn list_logs(&self, bucket_name: &str) -> Result<Vec<TxLogId>> {
        let mut current_tx = self.tx_provider.current();
        let state = self.state.lock();

        let mut log_id_set = state.list_logs(bucket_name)?;
        current_tx.data_with(|store_edit: &TxLogStoreEdit| {
            for (log_id, log_edit) in &store_edit.edit_table {
                match log_edit {
                    TxLogEdit::Create(_) => {
                        log_id_set.insert(log_id);
                    }
                    TxLogEdit::Append(_) => {}
                    TxLogEdit::Delete() => {
                        log_id_set.remove(log_id);
                    }
                }
            }
        });
        let log_id_vec = log_id_set.iter().collect();
        Ok(log_id_vec)
    }

    /// Creates a new, empty log in a bucket.
    /// 
    /// On success, the returned `TxLog` is opened in the appendable mode.
    /// 
    /// # Panics 
    ///
    /// This method must be called within a TX. Otherwise, this method panics. 
    pub fn create_log(&self, bucket: &str) -> Result<TxLog> {
        todo!()
    }

    /// Opens the log of a given ID.
    /// 
    /// For any log at any time, there can be at most one Tx that opens the log
    /// in the appendable mode.
    /// 
    /// # Panics 
    ///
    /// This method must be called within a TX. Otherwise, this method panics. 
    pub fn open_log(&self, log_id: TxLogId, can_append: bool) -> Result<TxLog> {
        let mut current_tx = self.tx_provider.current();
        let inner_log = self.open_inner_log(log_id, can_append, &mut current_tx)?;
        let tx_log = TxLog::new(inner_log, can_append);
        Ok(tx_log)
    }

    fn open_inner_log(
        &self,
        log_id: TxLogId,
        can_append: bool,
        current_tx: &mut CurrentTx<'_>
    ) -> Result<Arc<TxLogInner<D>>> {
        // Fast path: the log has been opened in this TX
        let has_opened = current_tx.data_with(|table: &OpenLogTable<D>| {
            table.log_table.get(&log_id).map(|log| log.clone())
        });
        if let Some(inner_log) = has_opened {
            return Ok(inner_log);
        }

        // Slow path: the first time a log is to be opened in a TX
        let state = self.state.lock();
        let log_entry = {
            // The log must exist in state...
            let log_entry = state.find_log(log_id)?;
            // ...and not be marked deleted by data.edit
            let is_deleted = current_tx.data_with(|store_edit: &TxLogStoreEdit| {
                store_edit.is_log_deleted(log_id)
            });
            if is_deleted {
                return Err(Error::NotFound);
            }
            log_entry
        };
        let bucket = log_entry.bucket.clone();
        let crypto_log = {
            let raw_log = self.raw_log_store.open_log(log_id, can_append)?;
            let key = log_entry.key;
            let root_meta = log_entry.root_meta;
            CryptoLog::open(raw_log, key, root_meta)?;
        };
        let tx_id = current_tx.id();
        let lazy_delete = {
            let raw_log_store = self.raw_log_store.clone();
            let lazy_delete = LazyDelete::new(log_id, move |log_id| {
                raw_log_store.delete_log(log_id).unwrap();
            });
            Arc::new(lazy_delete)
        };

        let inner_log = Arc::new(InnerLog {
            log_id,
            tx_id,
            bucket,
            crypto_log,
            lazy_delete,
        });

        current_tx.data_mut_with(|open_table: &mut OpenLogTable<D>| {
            open_table.log_table.insert(log_id, inner_log.clone())
        });
        Ok(inner_log)
    }

    /// Opens the log with the maximum ID in a bucket.
    /// 
    /// # Panics 
    ///
    /// This method must be called within a TX. Otherwise, this method panics. 
    pub fn open_log_in(&self, bucket: &str) -> Result<Arc<TxLog<S>>> {
        let log_ids = self.list_logs(bucket)?;
        let max_log_id = log_ids.iter()
            .max()
            .ok_or(Error::NotFound)?;
        self.open_log(max_log_id)
    }

    /// Checks whether the log of a given log ID exists or not.
    /// 
    /// # Panics 
    ///
    /// This method must be called within a TX. Otherwise, this method panics. 
    pub fn contains_log(&self, log_id: TxLogId) -> bool {
        let mut current_tx = self.tx_provider.current();
        let state = self.state.lock();
        self.do_contain_log(log_id, &state, &current_tx)
    }

    fn do_contain_log(&self, log_id: TxLogId, state: &State, current_tx: &CurrentTx<'_>) -> bool {
        if state.contains_log(log_id) {
            let not_deleted = current_tx.data_with(|store_edit: &TxLogStoreEdit| {
                !store_edit.is_log_deleted(log_id)
            });
            not_deleted
        } else {
            let is_created = current_tx.data_with(|store_edit: &TxLogStoreEdit| {
                store_edit.is_log_created(log_id)
            });
            is_created
        }
    }

    /// Deletes the log of a given ID.
    /// 
    /// # Panics 
    ///
    /// This method must be called within a TX. Otherwise, this method panics. 
    pub fn delete_log(&self, log_id: TxLogId) -> Result<()> {
        let mut current_tx = self.tx_provider.current();
        let state = self.state.lock();

        if !self.do_contain_log(log_id, &state, &current_tx) {
            return Err(Error::NotFound);
        }

        current_tx.data_mut_with(|store_edit: &mut TxLogStoreEdit| {
            store_edit.delete(log_id).unwrap()
        });
        current_tx.data_mut_with(|open_log_table: &mut OpenLogTable| {
            let inner_log_opt = open_log_table
                .log_table
                .remove(&log_id);
            let Some(inner_log) = inner_log_opt else {
                // The deleted log has not been opened
                return
            };
            LazyDelete::delete(&inner_log.lazy_delete);
        });
    }

    /// Returns the root key.
    pub fn key(&self) -> &Key {
        &self.key
    }
}

/// A transactional log.
#[derive(Clone)]
pub struct TxLog<D> {
    inner_log: Arc<InnerLog<D>>,
    can_append: bool,
}

struct TxLogInner<D> {
    log_id: TxLogId,
    tx_id: TxId,
    bucket: BucketName,
    crypto_log: CryptoLog<RawLog<D>>,
    lazy_delete: Arc<LazyDelete<RawLogId>>,
    is_dirty: bool,
}

impl TxLog {
    fn new(inner_log: Arc<InnerLog<D>>, can_append: bool) -> Self {
        Self {
            inner_log,
            can_append,
        }
    }

    /// Returns the log ID.
    pub fn id(&self) -> TxLogId {
        self.log_id
    }

    /// Returns the TX ID.
    pub fn tx_id(&self) -> TxId {
        self.tx_id
    }

    /// Returns the bucket that this log belongs to.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Returns whether the log is opened in the appendable mode.
    pub fn can_append(&self) -> bool {
        return self.can_append
    }

    /// Reads one or multiple data blocks at a specified position.
    /// 
    /// # Panics 
    ///
    /// This method must be called within a TX. Otherwise, this method panics. 
    pub fn read(&self, pos: BlockId, buf: &mut impl BlockBuf) -> Result<()> {
        self.inner_log.crypto_log.read(pos, buf)
    }

    /// Appends one or multiple data blocks at the end.
    /// 
    /// # Panics 
    ///
    /// This method must be called within a TX. Otherwise, this method panics. 
    pub fn append(&self, buf: &impl BlockBuf) -> Result<()> {
        if !self.can_append {
            return Err(Error::NotPermitted);
        }

        self.inner_log.is_dirty = true;
        self.inner_log.crypto_log.append(pos, buf)
    }

    /// Returns the length of the log in unit of block.
    /// 
    /// # Panics 
    ///
    /// This method must be called within a TX. Otherwise, this method panics. 
    pub fn nblocks(&self) -> usize {
        self.crypto_log.num_blocks()
    }
}

mod journaling {
    use crate::edit::{EditJournal, Edit, CompactPolicy};
    use super::super::chunk::ChunkAllocState;
    use super::super::raw_log::RawLogStoreState;
    use super::TxLogStoreState;

    pub type Journal<D> = EditJournal<AllState, D, JournalCompactPolicy>;

    pub struct AllState {
        pub chunk_alloc: ChunkAllocState,
        pub raw_log_store: RawLogStoreState, 
        pub tx_log_store: TxLogStoreState, 
    }

    impl<E: Edit<ChunkAllocState>> Edit<AllState> for E {
        fn apply_to(&mut self, state: &mut AllState) {
            self.apply_to(&mut state.chunk)
        }
    }

    impl<E: Edit<RawLogStoreState>> Edit<AllState> for E {
        fn apply_to(&mut self, state: &mut AllState) {
            self.apply_to(&mut state.raw_log_store)
        }
    }

    impl<E: Edit<TxLogStoreState>> Edit<PersistentState> for E {
        fn apply_to(&mut self, state: &mut AllState) {
            self.apply_to(&mut state.tx_log_store)
        }
    }

    pub type JournalCompactPolicy = NeverCompactPolicy;
}