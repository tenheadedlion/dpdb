//! > ** DP Database **
//!

mod cli;
mod db;
mod err;
mod net;
use err::Error;
//use log::{info, trace, warn};

fn main() {
    cli::init();
}
