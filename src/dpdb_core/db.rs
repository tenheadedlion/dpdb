use crate::cli::CF;
use crate::Executor;
use crate::Result;
use once_cell::sync::OnceCell;
use tokio::sync::Mutex;



pub static DB: OnceCell<Mutex<Executor>> = OnceCell::new();

pub async fn init() -> Result<()> {
    let opt = CF.get().unwrap();
    let executor = Executor::new(&opt.path).await?;
    let _ = DB.set(Mutex::new(executor));
    Ok(())
}
