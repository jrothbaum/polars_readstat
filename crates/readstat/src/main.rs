extern crate readstat_sys;

use clap::Parser;
use readstat::ReadStatCli;

fn main() {
    let args = ReadStatCli::parse();
    if let Err(e) = readstat::run(args) {
        eprintln!("Stopping with error: {}", e);
        std::process::exit(2);
    }
    std::process::exit(0);
}
