use super::state::TxLogStoreState;
use crate::layers::{
    edit::{CompactPolicy, Edit, EditJournal},
    log::{chunk::ChunkAllocState, raw_log::RawLogStoreState},
};

pub type Journal<D> = EditJournal<AllState, D, JournalCompactPolicy>;

pub type JournalCompactPolicy = NeverCompactPolicy;

pub struct AllState {
    pub chunk_alloc: ChunkAllocState,
    pub raw_log_store: RawLogStoreState,
    pub tx_log_store: TxLogStoreState,
}

impl<E: Edit<ChunkAllocState>> Edit<AllState> for E {
    fn apply_to(&mut self, state: &mut AllState) {
        self.apply_to(&mut state.chunk_alloc)
    }
}

impl<E: Edit<RawLogStoreState>> Edit<AllState> for E {
    fn apply_to(&mut self, state: &mut AllState) {
        self.apply_to(&mut state.raw_log_store)
    }
}

impl<E: Edit<TxLogStoreState>> Edit<AllState> for E {
    fn apply_to(&mut self, state: &mut AllState) {
        self.apply_to(&mut state.tx_log_store)
    }
}

struct NeverCompactPolicy;

impl<S> CompactPolicy for JournalCompactPolicy {
    fn on_commit_edits(&mut self, edits: &crate::layers::edit::EditGroup<S>) {}

    fn should_compact(&self) -> bool {
        false
    }

    fn done_compact(&mut self) {}
}
