extern crate timely;
extern crate differential_dataflow;
extern crate declarative_server;
extern crate serde_json;
extern crate websocket;

#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;

#[macro_use]
extern crate serde_derive;

use std::io::{Write, BufRead, BufReader, BufWriter};
use std::net::{TcpListener};
use std::sync::{Arc, Weak, Mutex};
use std::sync::mpsc::{Sender, Receiver};
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::cell::RefCell;
use std::fs::File;
use std::thread;

use timely::PartialOrder;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;
use timely::dataflow::scopes::root::Root;
use timely::dataflow::operators::Probe;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::generic::{source, OutputHandle, Operator};
use timely::dataflow::operators::Map;

use websocket::{Message, OwnedMessage};
use websocket::sync::Server;

use declarative_server::{Context, Plan, Rule, TxData, Out, Datom, Attribute, Value, setup_db, register};

#[derive(Clone)]
enum Interface { CLI, WS, TCP }

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Abomonation, Debug)]
struct Command {
    id: usize,
    // the worker (typically a controller) that issued this command
    // and is the one that should receive outputs
    owner: usize,
    cmd: String,
}

#[derive(Deserialize)]
enum Request {
    Transact { tx_data: Vec<TxData> },
    Register { query_name: String, plan: Plan, rules: Vec<Rule> },
    // LoadData { filename: String, max_lines: usize },
}

fn main () {
    
    // shared queue of commands to serialize (in the "put in an order" sense)
    let (send, recv) = std::sync::mpsc::channel();
    let input_recv = Arc::new(Mutex::new(recv));
    let shareable_input_recv = Arc::downgrade(&input_recv);

    // shared queue of outputs to forward to clients
    let (send_results, recv_results) = std::sync::mpsc::channel();
    let send_results_arc = Arc::new(Mutex::new(send_results));
    let send_results_shareable = Arc::downgrade(&send_results_arc);

    let interface: Interface = match std::env::args().nth(1) {
        None => Interface::CLI,
        Some(name) => {
            match name.as_ref() {
                "cli" => Interface::CLI,
                "ws" => Interface::WS,
                "tcp" => Interface::TCP,
                _ => panic!("Unknown interface {}", name)
            }
        }
    };
        
    let guards = timely::execute_from_args(std::env::args(), move |worker| {
        // setup interpreter context
        let mut ctx = worker.dataflow(|scope| {
            let (input_handle, db) = setup_db(scope);
            
            Context {
                db,
                input_handle,
                probes: Vec::new(),
                queries: HashMap::new(),
            }
        });

        // setup transaction context
        let mut next_tx: usize = 0;
        
        let timer = ::std::time::Instant::now();
        
        // common probe used by all dataflows to express progress information.
        let mut probe = timely::dataflow::operators::probe::Handle::new();
        
        // serializing and broadcasting commands
        let command_queue_strong = Rc::new(RefCell::new(VecDeque::new()));
        build_controller(worker, timer, shareable_input_recv.clone(), &command_queue_strong, &mut probe);
        let command_queue = Rc::downgrade(&command_queue_strong);
        drop(command_queue_strong);

        // continue running as long as we haven't dropped the queue
        while let Some(command_queue) = command_queue.upgrade() {

            if let Ok(mut borrow) = command_queue.try_borrow_mut() {
                while let Some(mut command) = borrow.pop_front() {

                    let index = worker.index();
                    println!("worker {:?}: received command: {:?}", index, command);

                    match serde_json::from_str::<Request>(&command.cmd) {
                        Err(msg) => { println!("failed to parse command: {:?}", msg); },
                        Ok(req) => {
                            match req {
                                Request::Transact { tx_data } => {
                                    
                                    for TxData(op, e, a, v) in tx_data {
                                        ctx.input_handle.update(Datom(e, a, v), op);
                                    }
                                    
                                    next_tx = next_tx + 1;
                                    ctx.input_handle.advance_to(next_tx);
                                    ctx.input_handle.flush();
                                },
                                Request::Register { query_name, plan, rules } => {

                                    let send_results_shareable = send_results_shareable.clone();
                                    
                                    worker.dataflow::<usize, _, _>(|scope| {
                                        let mut rel_map = register(scope, &mut ctx, &query_name, plan, rules);

                                        // output for experiments
                                        // let probe = rel_map.get_mut(&query_name).unwrap().trace.import(scope)
                                        //     .as_collection(|_,_| ())
                                        //     .consolidate()
                                        //     .inspect(move |x| println!("Nodes: {:?} (at {:?})", x.2, ::std::time::Instant::now()))
                                        //     .probe();

                                        // @TODO Frank sanity check
                                        let probe = rel_map.get_mut(&query_name).unwrap().trace.import(scope)
                                            .as_collection(|tuple,_| tuple.clone())
                                            .inner
                                            .map(|x| Out(x.0.clone(), x.2))
                                            .unary_notify(
                                                timely::dataflow::channels::pact::Exchange::new(move |_: &Out| command.owner as u64), 
                                                "OutputsRecv", 
                                                Vec::new(),
                                                move |input, _output: &mut OutputHandle<_, Out, _>, _notificator| {

                                                    // grab each command and queue it up
                                                    input.for_each(|_time, data| {
                                                        let out: Vec<Out> = data.drain(..).collect();

                                                        if let Some(send_results) = send_results_shareable.upgrade() {
                                                            if let Ok(send_results) = send_results.try_lock() {
                                                                send_results.send(out).expect("failed to put results onto channel");
                                                            }
                                                        } else { panic!("send_results channel not available"); }
                                                    });
                                                })
                                            .probe();

                                        ctx.probes.push(probe);
                                    });
                                },
                                // Request::LoadData { filename, max_lines } => {

                                //     let load_timer = ::std::time::Instant::now();
                                //     let peers = worker.peers();
                                //     let file = BufReader::new(File::open(filename).unwrap());
                                //     let mut line_count: usize = 0;

                                //     let attr_node: Attribute = 100;
                                //     let attr_edge: Attribute = 200;

                                //     for readline in file.lines() {
                                //         let line = readline.ok().expect("read error");

                                //         if line_count > max_lines { break; };
                                //         line_count += 1;
                                        
                                //         if !line.starts_with('#') && line.len() > 0 {
                                //             let mut elts = line[..].split_whitespace();
                                //             let src: u64 = elts.next().unwrap().parse().ok().expect("malformed src");
                                            
                                //             if (src as usize) % peers == index {
                                //                 let dst: u64 = elts.next().unwrap().parse().ok().expect("malformed dst");
                                //                 let typ: &str = elts.next().unwrap();
                                //                 match typ {
                                //                     "n" => { ctx.input_handle.update(Datom(src, attr_node, Value::Eid(dst)), 1); },
                                //                     "e" => { ctx.input_handle.update(Datom(src, attr_edge, Value::Eid(dst)), 1); },
                                //                     unk => { panic!("unknown type: {}", unk)},
                                //                 }
                                //             }
                                //         }
                                //     }

                                //     if index == 0 {
                                //         println!("{:?}:\tData loaded", load_timer.elapsed());
                                //         println!("{:?}", ::std::time::Instant::now());
                                //     }
                                    
                                //     next_tx = next_tx + 1;
                                //     ctx.input_handle.advance_to(next_tx);
                                //     ctx.input_handle.flush();
                                // }
                            }
                        }
                    }                    
                }
            }

            // @FRANK does the below make sense?

            worker.step();

            for probe in &mut ctx.probes {
                while probe.less_than(ctx.input_handle.time()) {
                    worker.step();
                }
            }
        }

        println!("worker {}: command queue unavailable; exiting command loop.", worker.index());
    });

    match interface {
        Interface::CLI => { run_cli_server(send, recv_results); },
        Interface::WS => { run_ws_server(send, recv_results); },
        Interface::TCP => { run_tcp_server(send, recv_results); },
    }
    
    drop(input_recv);
    drop(send_results_arc);
    
    guards.unwrap();
}

/// The controller is a differential dataflow serializing and
/// circulating new commands to all workers.
fn build_controller<A: timely::Allocate>(
    worker: &mut Root<A>,
    timer: ::std::time::Instant,
    input_recv: Weak<Mutex<Receiver<String>>>,
    command_queue: &Rc<RefCell<VecDeque<Command>>>,
    handle: &mut ProbeHandle<Product<RootTimestamp, usize>>,
) {

    let this_idx = worker.index();
    let command_queue = command_queue.clone();

    // command serialization and circulation
    worker.dataflow(move |dataflow| {

        let peers = dataflow.peers();
        let mut recvd_commands = Vec::new();

        // source attempting to pull from input_recv and producing
        // commands for everyone
        source(dataflow, "InputCommands", move |capability| {

            // so we can drop, if input queue vanishes.
            let mut capability = Some(capability);

            // closure broadcasts any commands it grabs.
            move |output| {

                if let Some(input_recv) = input_recv.upgrade() {

                    // determine current nanoseconds
                    if let Some(capability) = capability.as_mut() {

                        // this could be less frequent if needed.
                        let mut time = capability.time().clone();
                        let elapsed = timer.elapsed();
                        time.inner = (elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64) as usize;

                        // downgrade the capability.
                        capability.downgrade(&time);

                        if let Ok(input_recv) = input_recv.try_lock() {
                            while let Ok(cmd) = input_recv.try_recv() {
                                let mut session = output.session(&capability);
                                for worker_idx in 0 .. peers {
                                    // @TODO command ids?
                                    session.give((worker_idx, Command { id: 0, owner: this_idx, cmd: cmd.clone() }));
                                }
                            }
                        }
                    } else { panic!("command serializer: capability lost while input queue valid"); }
                } else { capability = None; }
            }
        })
        .unary_notify(
            timely::dataflow::channels::pact::Exchange::new(|x: &(usize, Command)| x.0 as u64), 
            "InputCommandsRecv", 
            Vec::new(), 
            move |input, output, notificator| {

            // grab all commands
            input.for_each(|time, data| {
                recvd_commands.extend(data.drain(..).map(|(_,command)| (time.time().clone(), command)));
                if false { output.session(&time).give(0u64); }
            });

            recvd_commands.sort();

            // try to move any commands at completed times to a shared queue.
            if let Ok(mut borrow) = command_queue.try_borrow_mut() {
                while recvd_commands.len() > 0 && !notificator.frontier(0).iter().any(|x| x.less_than(&recvd_commands[0].0)) {
                    borrow.push_back(recvd_commands.remove(0).1);
                }
            } else { panic!("failed to borrow shared command queue"); }
        })
        .probe_with(handle);
    });
}

fn run_cli_server(command_channel: Sender<String>, results_channel: Receiver<Vec<Out>>) {

    println!("[CLI-SERVER] running");

    std::io::stdout().flush().unwrap();
    let input = std::io::stdin();

    thread::spawn(move || {
        loop {
            match results_channel.recv() {
                Err(_err) => break,
                Ok(results) => { println!("=> {:?}", results) }
            };
        }
    });
    
    loop {
        if let Some(line) = input.lock().lines().map(|x| x.unwrap()).next() {
            match line.as_str() {
                "exit" => { break },
                _ => { command_channel.send(line).expect("failed to send command"); },
            }
        }
    }

    println!("[CLI-SERVER] exiting");
}

fn run_ws_server(command_channel: Sender<String>, results_channel: Receiver<Vec<Out>>) {
    
    let send_handle = &command_channel;
    let mut server = Server::bind("127.0.0.1:6262").expect("can't bind to port");

    println!("[WS-SERVER] running on port 6262");
    
    match server.accept() {
        Err(_err) => println!("[WS-SERVER] failed to accept"),
        Ok(ws_upgrade) => {
            let client = ws_upgrade.accept().expect("[WS-SERVER] failed to accept");

            println!("[WS-SERVER] connection from {:?}", client.peer_addr().unwrap());

            let (mut receiver, mut sender) = client.split().unwrap();

            thread::spawn(move || {
                loop {
                    match results_channel.recv() {
                        Err(_err) => break,
                        Ok(results) => {
                            let serialized = serde_json::to_string::<Vec<Out>>(&results).expect("failed to serialize outputs");
                            sender.send_message(&Message::text(serialized)).expect("failed to send message");
                        }
                    };
                }
            });

            for msg in receiver.incoming_messages() {
                
                let msg = msg.unwrap();
                
                println!("[WS-SERVER] new message: {:?}", msg);

                match msg {
                    OwnedMessage::Close(_) => { println!("[WS-SERVER] client closed connection"); },
                    OwnedMessage::Text(line) => { send_handle.send(line).expect("failed to send command"); },
                    _ => {  },
                }
            }
        }
    }

    println!("[WS-SERVER] exited");
}

fn run_tcp_server(command_channel: Sender<String>, results_channel: Receiver<Vec<Out>>) {

    let send_handle = &command_channel;
    
    let listener = TcpListener::bind("127.0.0.1:6262").expect("can't bind to port");
    listener.set_nonblocking(false).expect("Cannot set blocking");

    println!("[TCP-SERVER] running on port 6262");
    
    match listener.accept() {
        Ok((stream, _addr)) => {
            
            println!("[TCP-SERVER] accepted connection");

            let mut out_stream = stream.try_clone().unwrap();
            let mut writer = BufWriter::new(out_stream);
            
            thread::spawn(move || {
                loop {
                    match results_channel.recv() {
                        Err(_err) => break,
                        Ok(results) => {
                            let serialized = serde_json::to_string::<Vec<Out>>(&results)
                                .expect("failed to serialize outputs");
                            
                            writer.write(serialized.as_bytes()).expect("failed to send output");
                        }
                    };
                }
            });
            
            let mut reader = BufReader::new(stream);
            for input in reader.lines() {
                match input {
                    Err(e) => { println!("Error reading line {}", e); break; },
                    Ok(line) => {
                        println!("[TCP-SERVER] new message: {:?}", line);
                        
                        send_handle.send(line).expect("failed to send command");
                    }
                }
            }

            println!("[TCP-SERVER] closing connection");
        },
        Err(e) => { println!("Encountered I/O error: {}", e); }
    }
    
    println!("[TCP-SERVER] exited");
}
