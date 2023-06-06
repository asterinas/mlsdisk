use core::ops::{Deref, DerefMut};

use serde::{Serialize, Deserialize};

/// An edit to an object.
pub trait Edit: Serialize + Deserialize {
    type Object: Object;
    type Error;

    /// Apply this edit to an object.
    fn apply_to(&self, object: &mut Self::Object) -> Result<(), Self::Error>;
}

/// An editable object.
pub trait Object: Serialize + Deserialize {
}

/// A group of edits.
/// 
/// A group of edits is also viewed an edit. This could be convenient when
/// a group of edits should be treated as an atomic edit.
pub struct EditGroup<E> {
    edits: Vec<E>,
}

impl<E: Edit> EditGroup<E> {
    /// Creates a new instance.
    pub fn new() -> Self {
        Self { edits: Vec::new(), }
    }

    /// Adds an edit to the group.
    pub fn add(&mut self, edit: E) {
        self.edits.push_back(edit)
    }

    /// Returns the number of edits in the group.
    pub fn len(&self) -> usize {
        self.edits.len()
    }

    /// Clears the group, removing all edits.
    pub fn clear(&mut self) {
        self.edits.clear()
    }

    /// Returns a slice of the edits in the group.
    pub fn as_slice(&self) -> &[E] {
        self.edits.as_slice()
    }
    
    /// Returns a mutable slice of the edits in the group.
    pub fn as_slice_mut(&mut self) -> &mut [E] {
        self.edits.as_slice_mut()
    }
}

impl<E> Deref for EditGroup<E> {
    type Target = [E];

    #[inline]
    fn deref(&self) -> &[E] {
        self.as_slice()
    }
}

impl<E> DerefMut for EditGroup<E> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [E] {
        self.as_slice_mut()
    }
}

impl<E: Edit> Edit for EditGroup<E> {
    type Object = <E as Edit>::Object;
    type Error = <E as Edit>::Error;

    fn apply_to(&self, object: &mut Self::Object) -> Result<(), Self::Error> {
        for edit in &self.edits {
            // TODO: what if one edit fails to apply? need to rollback applied
            // edits?
            edit.apply_to(object)?;
        }
    }
}