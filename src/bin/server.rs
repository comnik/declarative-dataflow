extern crate timely;
extern crate differential_dataflow;
extern crate declarative_server;
extern crate serde_json;
extern crate websocket;

#[macro_use]
extern crate abomonation_derive;
extern crate abomonation;

use std::io::{Write, BufRead, BufReader};
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
use timely::dataflow::operators::Unary;
use timely::dataflow::operators::Probe;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::generic::{source, OutputHandle};
use timely::dataflow::operators::Map;

use differential_dataflow::operators::consolidate::Consolidate;

use websocket::{Message, OwnedMessage};
use websocket::sync::Server;

use declarative_server::{Context, Plan, Rule, TxData, Out, Datom, Attribute, Value, setup_db, register};

const ATTR_NODE: Attribute = 100;
const ATTR_EDGE: Attribute = 200;

#[derive(Clone)]
enum Interface { CLI, WS, }

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Abomonation, Debug)]
struct Command {
    id: usize,
    // the worker (typically a controller) that issued this command
    // and is the one that should receive outputs
    owner: usize,
    cmd: Vec<String>,
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

                    if command.cmd.len() > 1 {
                        let operation = command.cmd.remove(0);
                        match operation.as_str() {
                            "register" => {
                                if command.cmd.len() > 2 {
                                    let query_name = command.cmd.remove(0);
                                    let plan_json = command.cmd.remove(0);
                                    let rules_json = command.cmd.remove(0);

                                    match serde_json::from_str::<Plan>(&plan_json) {
                                        Err(msg) => { println!("{:?}", msg); },
                                        Ok(plan) => {
                                            let rules = serde_json::from_str::<Vec<Rule>>(&rules_json).unwrap();
                                            let send_results_shareable = send_results_shareable.clone();
                                                
                                            worker.dataflow::<usize, _, _>(|scope| {
                                                let mut rel_map = register(scope,
                                                                           &mut ctx,
                                                                           &query_name,
                                                                           plan,
                                                                           rules);

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
                                        }
                                    }
                                } else {
                                    println!("Command does not conform to register?<name>?<plan>?<rules>");
                                }
                            },
                            "transact" => {
                                if command.cmd.len() > 0 {
                                    let tx_data_json = command.cmd.remove(0);

                                    match serde_json::from_str::<Vec<TxData>>(&tx_data_json) {
                                        Err(msg) => { println!("{:?}", msg); },
                                        Ok(tx_data) => {
                                            println!("{:?}", tx_data);
                                            for TxData(op, e, a, v) in tx_data {
                                                ctx.input_handle.update(Datom(e, a, v), op);
                                            }
                                            next_tx = next_tx + 1;
                                            ctx.input_handle.advance_to(next_tx);
                                            ctx.input_handle.flush();
                                        }
                                    }
                                } else {
                                    println!("No tx-data provided");
                                }
                            },
                            "load_data" => {
                                if command.cmd.len() > 0 {
                                    let load_timer = ::std::time::Instant::now();
                                    
                                    let filename = command.cmd.remove(0);
                                    let file = BufReader::new(File::open(filename).unwrap());
                                    let peers = worker.peers();

                                    let max_lines: usize = command.cmd.remove(0).parse().unwrap();
                                    let mut line_count: usize = 0;
                                    
                                    for readline in file.lines() {
                                        let line = readline.ok().expect("read error");

                                        if line_count > max_lines { break; };
                                        line_count += 1;
                                        
                                        if !line.starts_with('#') && line.len() > 0 {
                                            let mut elts = line[..].split_whitespace();
                                            let src: u64 = elts.next().unwrap().parse().ok().expect("malformed src");
                                            
                                            if (src as usize) % peers == index {
                                                let dst: u64 = elts.next().unwrap().parse().ok().expect("malformed dst");
                                                let typ: &str = elts.next().unwrap();
                                                match typ {
                                                    "n" => { ctx.input_handle.update(Datom(src, ATTR_NODE, Value::Eid(dst)), 1); },
                                                    "e" => { ctx.input_handle.update(Datom(src, ATTR_EDGE, Value::Eid(dst)), 1); },
                                                    unk => { panic!("unknown type: {}", unk)},
                                                }
                                            }
                                        }
                                    }

                                    if index == 0 {
                                        println!("{:?}:\tData loaded", load_timer.elapsed());
                                        println!("{:?}", ::std::time::Instant::now());
                                    }
                                    
                                    next_tx = next_tx + 1;
                                    ctx.input_handle.advance_to(next_tx);
                                    ctx.input_handle.flush();

                                } else {
                                    println!("No filename provided");
                                }
                            },
                            _ => {
                                println!("worker {:?}: unrecognized command: {:?}", index, operation);
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

    let forwarder_iface = interface.clone();
    thread::spawn(|| run_forwarder(forwarder_iface, recv_results));

    run_server(interface, send);
    
    drop(input_recv);
    drop(send_results_arc);
    
    guards.unwrap();
}

/// The controller is a differential dataflow serializing and
/// circulating new commands to all workers.
fn build_controller<A: timely::Allocate>(
    worker: &mut Root<A>,
    timer: ::std::time::Instant,
    input_recv: Weak<Mutex<Receiver<Vec<String>>>>,
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
                                // @TODO load balance instead of replicate?
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

fn run_server(interface: Interface, command_channel: Sender<Vec<String>>) {

    println!("[SERVER] running");
    
    match interface {
        Interface::CLI => {
            std::io::stdout().flush().unwrap();
            
            let input = std::io::stdin();
            let mut done = false;

            while !done {
                if let Some(line) = input.lock().lines().map(|x| x.unwrap()).next() {
                    let elts: Vec<_> = line.split('?').map(|x| x.to_owned()).collect();

                    if elts.len() > 0 {
                        match elts[0].as_str() {
                            "help" => { println!("valid commands are currently: help, register, transact, load_data, exit"); },
                            "register" => { command_channel.send(elts).expect("failed to send command"); },
                            "transact" => { command_channel.send(elts).expect("failed to send command"); },
                            "load_data" => { command_channel.send(elts).expect("failed to send command"); },
                            "exit" => { done = true; },
                            _ => { println!("unrecognized command: {:?}", elts[0]); },
                        }
                    }

                    std::io::stdout().flush().unwrap();
                }
            }
        },
        Interface::WS => {
            let send_handle = &command_channel;
            let mut server = Server::bind("127.0.0.1:6262").expect("[SERVER] can't bind to port");

            let help_msg = Message::text("valid commands are currently: help, register, transact, exit");
            let ok_msg = Message::text("ok");
            let unrecognized_command_msg = Message::text("unrecognized command");
            let malformed_msg = Message::text("malformed");
            let unknown_type_msg = Message::text("serrver only accepts string messages");
            let close_msg = Message::close();
            
            loop {
                match server.accept() {
                    Err(_err) => println!("[SERVER] failed to accept"),
                    Ok(ws_upgrade) => {
                        let client = ws_upgrade.accept().expect("[SERVER] failed to accept");

                        println!("[SERVER] connection from {:?}", client.peer_addr().unwrap());

                        let (mut receiver, mut sender) = client.split().unwrap();

                        for msg in receiver.incoming_messages() {

                            let msg = msg.unwrap();
                            println!("[SERVER] new message: {:?}", msg);

                            match msg {
                                OwnedMessage::Close(_) => { println!("[SERVER] client closed connection"); },
                                OwnedMessage::Text(line) => {
                                    let elts: Vec<_> = line.split('?').map(|x| x.to_owned()).collect();

                                    if elts.len() > 0 {
                                        match elts[0].as_str() {
                                            "help" => { sender.send_message(&help_msg).unwrap(); },
                                            "register" => {
                                                send_handle.send(elts).expect("failed to send command");
                                                sender.send_message(&ok_msg).unwrap();
                                            },
                                            "transact" => {
                                                send_handle.send(elts).expect("failed to send command");
                                                sender.send_message(&ok_msg).unwrap();
                                            },
                                            "exit" => { sender.send_message(&close_msg).unwrap(); },
                                            _ => { sender.send_message(&unrecognized_command_msg).unwrap(); },
                                        }
                                    } else { sender.send_message(&malformed_msg).unwrap(); }
                                },
                                _ => { sender.send_message(&unknown_type_msg).unwrap(); },
                            }
                        }
                    }
                }
            }
        }
    };

    println!("[SERVER] exited");
}

fn run_forwarder(interface: Interface, results_channel: Receiver<Vec<Out>>) {

    println!("[FORWARDER] running");
    
    match interface {
        Interface::CLI => {
            loop {
                match results_channel.recv() {
                    Err(_err) => break,
                    Ok(results) => { println!("=> {:?}", results) }
                };
            }
        },
        Interface::WS => {
            loop {
                
            }
        }
    };

    println!("[FORWARDER] exited");
}
