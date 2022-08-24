//! > ** DP Database **
//!

pub mod dpdb_core;
pub use dpdb_core::*;
mod cli;

fn main() {
    cli::init();
}
