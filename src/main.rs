//! > ** DP Database **
//!

mod dpdb_core;
use dpdb_core::*;
mod cli;
mod net;
use log::{info, trace, warn};

fn main() {
    cli::init();
}
