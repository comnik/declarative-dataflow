extern crate timely;
extern crate differential_dataflow;
extern crate declarative_server;
extern crate serde_json;
extern crate ws;

use std::io::{Write, BufRead, BufReader};
use std::sync::{Arc, Weak, Mutex};
use std::sync::mpsc::Receiver;
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;
use std::cell::RefCell;
use std::fs::File;

use timely::PartialOrder;
use timely::progress::timestamp::RootTimestamp;
use timely::progress::nested::product::Product;
use timely::dataflow::scopes::root::Root;
use timely::dataflow::operators::Unary;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::Probe;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::generic::source;

use differential_dataflow::operators::consolidate::Consolidate;

use ws::{Message, CloseCode};

use declarative_server::{Context, Plan, TxData, Datom, Attribute, Value, setup_db, register};

const ATTR_NODE: Attribute = 100;
const ATTR_EDGE: Attribute = 200;

enum Interface { CLI, WS, }

fn main () {
    
    // shared queue of commands to serialize (in the "put in an order" sense).
    let (send, recv) = std::sync::mpsc::channel();
    let recv = Arc::new(Mutex::new(recv));
    let weak = Arc::downgrade(&recv);

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
        println!("Setting-up interpreter context");
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
        
        // queue shared between serializer (producer) and command loop (consumer).
        let command_queue_strong = Rc::new(RefCell::new(VecDeque::new()));
        build_command_serializer(worker, timer, weak.clone(), &command_queue_strong, &mut probe);
        let command_queue = Rc::downgrade(&command_queue_strong);
        drop(command_queue_strong);

        // continue running as long as we haven't dropped the queue.
        while let Some(command_queue) = command_queue.upgrade() {

            if let Ok(mut borrow) = command_queue.try_borrow_mut() {
                while let Some(mut command) = borrow.pop_front() {

                    let index = worker.index();
                    println!("worker {:?}: received command: {:?}", index, command);

                    if command.len() > 1 {
                        let operation = command.remove(0);
                        match operation.as_str() {
                            "register" => {
                                if command.len() > 0 {
                                    let plan_json = command.remove(0);

                                    match serde_json::from_str::<Plan>(&plan_json) {
                                        Err(msg) => { println!("{:?}", msg); },
                                        Ok(plan) => {
                                            worker.dataflow::<usize, _, _>(|scope| {
                                                let mut rel_map = register(scope, &mut ctx, "test".to_string(), plan);
                                                let probe = rel_map.get_mut("test").unwrap().trace.import(scope)
                                                // .as_collection(|tuple, _| tuple.clone())
                                                    .as_collection(|_,_| ())
                                                    .consolidate()
                                                    .inspect(move |x| println!("Nodes: {:?} (at {:?})", x.2, ::std::time::Instant::now()))
                                                // .inspect_batch(move |_t, tuples| {
                                                //     let out: Vec<Out> = tuples.into_iter()
                                                //         .map(move |x| Out(x.0.clone(), x.2))
                                                //         .collect();

                                                //     // @TODO how to output?
                                                //     println!("<= {:?} {:?}", &name, out);
                                                // })
                                                    .probe();

                                                ctx.probes.push(probe);
                                            });
                                        }
                                    }
                                } else {
                                    println!("No plan provided");
                                }
                            },
                            "transact" => {
                                if command.len() > 0 {
                                    let tx_data_json = command.remove(0);

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
                                if command.len() > 0 {
                                    let load_timer = ::std::time::Instant::now();
                                    
                                    let filename = command.remove(0);
                                    let file = BufReader::new(File::open(filename).unwrap());
                                    let peers = worker.peers();

                                    let max_lines: usize = command.remove(0).parse().unwrap();
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

    std::io::stdout().flush().unwrap();
    
    match interface {
        Interface::CLI => {
            let input = std::io::stdin();
            let mut done = false;

            while !done {
                if let Some(line) = input.lock().lines().map(|x| x.unwrap()).next() {
                    let elts: Vec<_> = line.split('?').map(|x| x.to_owned()).collect();

                    if elts.len() > 0 {
                        match elts[0].as_str() {
                            "help" => { println!("valid commands are currently: help, register, transact, load_data, exit"); },
                            "register" => { send.send(elts).expect("failed to send command"); },
                            "transact" => { send.send(elts).expect("failed to send command"); },
                            "load_data" => { send.send(elts).expect("failed to send command"); },
                            "exit" => { done = true; },
                            _ => { println!("unrecognized command: {:?}", elts[0]); },
                        }
                    }

                    std::io::stdout().flush().unwrap();
                }
            }

            drop(send);
        },
        Interface::WS => {
            let send_handle = &send;
            
            ws::listen("127.0.0.1:6262", |out| {
                move |msg| {
                    match msg {
                        Message::Text(line) => {
                            let elts: Vec<_> = line.split('?').map(|x| x.to_owned()).collect();

                            if elts.len() > 0 {
                                match elts[0].as_str() {
                                    "help" => { out.send("valid commands are currently: help, register, transact, exit") },
                                    "register" => {
                                        send_handle.send(elts).expect("failed to send command");
                                        out.send("Ok")
                                    },
                                    "transact" => {
                                        send_handle.send(elts).expect("failed to send command");
                                        out.send("Ok")
                                    },
                                    "exit" => { out.close(CloseCode::Normal) },
                                    _ => { out.send("unrecognized command") },
                                }
                            } else {
                                out.send("malformed message")
                            }
                        },
                        Message::Binary(_) => { out.send("Server only accepts string messages.") },
                    }
                }
            }).unwrap();
        }
    };
    
    println!("main: exited command loop");
    drop(recv);

    guards.unwrap();
}

fn build_command_serializer<A: timely::Allocate>(
    worker: &mut Root<A>,
    timer: ::std::time::Instant,
    input: Weak<Mutex<Receiver<Vec<String>>>>,
    target: &Rc<RefCell<VecDeque<Vec<String>>>>,
    handle: &mut ProbeHandle<Product<RootTimestamp, usize>>,
) {

    let target = target.clone();

    // build a dataflow used to serialize and circulate commands
    worker.dataflow(move |dataflow| {

        let peers = dataflow.peers();
        let mut recvd = Vec::new();

        // a source that attempts to pull from `recv` and produce commands for everyone
        source(dataflow, "InputCommands", move |capability| {

            // so we can drop, if input queue vanishes.
            let mut capability = Some(capability);

            // closure broadcasts any commands it grabs.
            move |output| {

                if let Some(input) = input.upgrade() {

                    // determine current nanoseconds
                    if let Some(capability) = capability.as_mut() {

                        // this could be less frequent if needed.
                        let mut time = capability.time().clone();
                        let elapsed = timer.elapsed();
                        time.inner = (elapsed.as_secs() * 1000000000 + elapsed.subsec_nanos() as u64) as usize;

                        // downgrade the capability.
                        capability.downgrade(&time);

                        if let Ok(input) = input.try_lock() {
                            while let Ok(command) = input.try_recv() {
                                let command: Vec<String> = command;
                                let mut session = output.session(&capability);
                                for worker_index in 0 .. peers {
                                    session.give((worker_index, command.clone()));
                                }
                            }
                        }
                    }
                    else { panic!("command serializer: capability lost while input queue valid"); }
                }
                else {
                    capability = None;
                }
            }
        })
        .unary_notify(
            Exchange::new(|x: &(usize, Vec<String>)| x.0 as u64), 
            "InputCommandsRecv", 
            Vec::new(), 
            move |input, output, notificator| {

            // grab each command and queue it up
            input.for_each(|time, data| {
                recvd.extend(data.drain(..).map(|(_,command)| (time.time().clone(), command)));
                if false { output.session(&time).give(0u64); }
            });

            recvd.sort();

            // try to move any commands at completed times to a shared queue.
            if let Ok(mut borrow) = target.try_borrow_mut() {
                while recvd.len() > 0 && !notificator.frontier(0).iter().any(|x| x.less_than(&recvd[0].0)) {
                    borrow.push_back(recvd.remove(0).1);
                }
            }
            else { panic!("failed to borrow shared command queue"); }

        })
        .probe_with(handle);
    });
}
