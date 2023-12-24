pub(crate) use crate::error::{Errno::*, Error};
pub(crate) use crate::layers::bio::{BlockId, BLOCK_SIZE};
pub(crate) use crate::os::{Arc, Box, String, ToString, Vec, Weak};
pub(crate) use crate::util::{
    align_down, align_up, Aead as _, RandomInit, Rng as _, Skcipher as _,
};
pub(crate) use crate::{return_errno, return_errno_with_msg};

pub(crate) type Result<T> = core::result::Result<T, Error>;

pub(crate) use core::fmt::{self, Debug};
#[cfg(not(feature = "linux"))]
pub(crate) use log::{debug, error, info, trace, warn};

#[cfg(feature = "linux")]
pub(crate) use crate::vec;
