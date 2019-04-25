//! 3DF CLI. Each instance can act as either a producer or a consumer
//! of data.

#[macro_use]
extern crate log;
#[macro_use]
extern crate clap;

use std::io::Read;

use clap::App;
use uuid::Uuid;
use ws::{connect, CloseCode};

use declarative_dataflow::plan::{GraphQl, Plan};
use declarative_dataflow::server::{Register, Request};
use declarative_dataflow::{Rule, TxData};

fn main() {
    env_logger::init();

    let cli_config = load_yaml!("cli.yml");
    let matches = App::from_yaml(cli_config).get_matches();

    let host = matches.value_of("host").unwrap_or("127.0.0.1");
    let port = matches.value_of("port").unwrap_or("6262");
    let addr = format!("ws://{}:{}", host, port);

    if let Some(matches) = matches.subcommand_matches("ping") {
        connect(addr.clone(), |out| {
            let req = serde_json::to_string::<Vec<Request>>(&vec![Request::Status])
                .expect("failed to serialize request");

            out.send(req).unwrap();

            move |msg| {
                info!("{:?}", msg);
                out.close(CloseCode::Normal)
            }
        })
        .expect("failed to connect");
    }

    if let Some(matches) = matches.subcommand_matches("tx") {
        connect(addr.clone(), |out| {
            let tx_data: Vec<TxData> = match matches.value_of("TXDATA") {
                None => {
                    let mut buf = String::new();
                    std::io::stdin()
                        .read_to_string(&mut buf)
                        .expect("failed to read from stdin");

                    serde_json::from_str(&buf).expect("failed to parse tx data")
                }
                Some(tx_in) => serde_json::from_str(tx_in).expect("failed to parse tx data"),
            };

            let req = serde_json::to_string::<Vec<Request>>(&vec![Request::Transact(tx_data)])
                .expect("failed to serialize request");

            debug!("{:?}", req);

            out.send(req).unwrap();

            move |msg| {
                info!("{:?}", msg);
                out.close(CloseCode::Normal)
            }
        })
        .expect("failed to connect");
    }

    if let Some(matches) = matches.subcommand_matches("gql") {
        connect(addr.clone(), |out| {
            let query: String = match matches.value_of("QUERY") {
                None => {
                    let mut buf = String::new();
                    std::io::stdin()
                        .read_to_string(&mut buf)
                        .expect("failed to read from stdin");

                    buf
                }
                Some(query) => query.to_string(),
            };

            let name = Uuid::new_v4();

            let req = serde_json::to_string::<Vec<Request>>(&vec![Request::Register(Register {
                rules: vec![Rule {
                    name: name.to_string(),
                    plan: Plan::GraphQl(GraphQl::new(query)),
                }],
                publish: vec![name.to_string()],
            })])
            .expect("failed to serialize request");

            debug!("{:?}", req);

            out.send(req).unwrap();

            move |msg| {
                info!("{:?}", msg);
                out.close(CloseCode::Normal)
            }
        })
        .expect("failed to connect");
    }
}
