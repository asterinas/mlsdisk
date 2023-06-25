use core::iter::Iterator;
use serde::{Serialize, Deserialize};

/// An edit of `Edit<S>` is an incremental change to a state of `S`.
pub trait Edit<S>: Serialize + for<'de> Deserialize<'de> {
    /// Apply this edit to a state.
    fn apply_to(&self, state: &mut S);
}

/// A group of edits to a state.
pub struct EditGroup<S: Sized> {
    edits: Vec<Box<dyn Edit<S>>>,
}

impl<S: Sized> EditGroup<S> {
    /// Creates an empty edit group.
    pub fn new() -> Self {
        Self {
            edits: Vec::new(),
        }
    }

    /// Adds an edit to the group.
    pub fn push<E: Edit<S> + 'static>(&mut self, edit: E) {
        self.edits.push(Box::new(edit) as Box<dyn Edit<S>>);
    }

    /// Returns an iterator to the contained edits.
    pub fn iter(&self) -> impl Iterator<Item = &dyn Edit<S>> {
        self.edits.iter().map(|boxed| &**boxed)
    }

    /// Clears the edit group by removing all contained edits.
    pub fn clear(&mut self) {
        self.edits.clear()
    }

    /// Returns whether the edit group contains no edits.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the length of the edit group.
    pub fn len(&self) -> usize {
        self.edits.len()
    }
}

impl<S: Sized> Edit<S> for EditGroup<S> {
    fn apply_to(&self, state: &mut S) {
        for edit in &self.edits {
            edit.apply_to(state);
        }
    }
}
