//! The layer of edit journal.

mod edit;
mod journal;

pub use self::edit::{Edit, EditGroup};
pub use self::journal::{
    CompactPolicy, DefaultCompactPolicy, EditJournal, EditJournalMeta, NeverCompactPolicy,
};
