// See https://docs.rs/futures/latest/futures/executor/fn.enter.html for reference

use std::cell::Cell;
use std::fmt;

thread_local!(static ENTERED: Cell<bool> = Cell::new(false));

pub struct Enter;

pub struct EnterError;

impl fmt::Debug for EnterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnterError").finish()
    }
}

impl fmt::Display for EnterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "an execution scope has already been entered")
    }
}

impl std::error::Error for EnterError {}

pub fn enter() -> Result<Enter, EnterError> {
    ENTERED.with(|c| {
        if c.get() {
            Err(EnterError)
        } else {
            c.set(true);

            Ok(Enter)
        }
    })
}

impl fmt::Debug for Enter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Enter").finish()
    }
}

impl Drop for Enter {
    fn drop(&mut self) {
        ENTERED.with(|c| {
            assert!(c.get());
            c.set(false);
        });
    }
}
