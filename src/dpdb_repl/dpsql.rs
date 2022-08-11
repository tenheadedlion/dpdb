use rustyline::error::ReadlineError;
use rustyline::{Editor, Result};

fn main() -> Result<()> {
    // `()` can be used when no completer is required
    let mut rl = Editor::<()>::new()?;
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    let mut executor = dpdb::Executor::new().expect("this should not fail");
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                match executor.execute(&line) {
                    Ok(report) => {
                        if let Some(msg) = report.msg {
                            println!("=> {msg}");
                        }
                        println!("{:?}", report.time_elapsed);
                    }
                    Err(err) => println!("{err}"),
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history("history.txt")
}
