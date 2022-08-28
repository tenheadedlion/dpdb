//! > ** DP Database **
//!

pub mod dpdb_core;
pub use dpdb_core::*;
mod cli;
mod net;
pub use log::{info, trace, warn};

fn main() {
    cli::init();
}
