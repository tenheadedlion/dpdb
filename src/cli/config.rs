use once_cell::sync::OnceCell;

pub static CF: OnceCell<Config> = OnceCell::new();

#[derive(Clone, Debug)]
pub struct Config {
    pub path: String,
}

pub fn init(matches: &clap::ArgMatches) {
    let path = matches.value_of("path").unwrap().to_owned();
    let _ = CF.set(Config { path });
}
