//! 3DF CLI. Each instance can act as either a producer or a consumer
//! of data.

#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;

use clap::App;

use ws::{connect, CloseCode};

use declarative_dataflow::server::Request;

fn main() {
    env_logger::init();

    let cli_config = load_yaml!("cli.yml");
    let matches = App::from_yaml(cli_config).get_matches();

    if let Some(matches) = matches.subcommand_matches("ping") {
        connect("ws://127.0.0.1:6262", |out| {
            let req = serde_json::to_string::<Vec<Request>>(&vec![Request::Status])
                .expect("failed to serialize request");

            out.send(req).unwrap();

            move |msg| {
                println!("Got message: {}", msg);
                out.close(CloseCode::Normal)
            }
        })
        .expect("failed to connect");
    }
}
