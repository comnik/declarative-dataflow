//! A dataflow operator sending outputs back to clients.

/// Extension trait for `Stream`.
pub trait Output<A: Allocate, Token: Hash+From<usize>> {
    /// Consumes results and pushes them to clients via WebSockets.
    fn output<'a>(&self, server: &Server<Token>);
}

impl<'a,
     A: Allocate,
     Token: Hash+From<usize>> Handler<A, Token> for Stream<Child<'a, Worker<A>, u64>, (usize, super::Output)> {

    fn output<'b>(&self, server: &Server<Token>) {        
        self.sink(
            Exchange::new(|&(owner, _)| owner as u64),
            "Output",
            move |input| {

                info!("[WORKER {}] {:?} {:?}", worker_index, query_name, results);
                
                input.for_each(|_time, data| {

                    for (_owner, query, results)
                    let out: Vec<Output> = data.iter()
                        .map(|(tuple, t, diff)| (tuple.clone(), *diff, *t))
                        .collect();

                    send_results_handle.send((name.clone(), out)).unwrap();

                    match server.interests.get(&query_name) {
                        None => {
                            /* @TODO unregister this flow */
                            info!("NO INTEREST FOR THIS RESULT");
                        }
                        Some(tokens) => {
                            let serialized = serde_json::to_string::<(String, Vec<Output>)>(
                                &(query_name, results),
                            ).expect("failed to serialize outputs");
                            let msg = ws::Message::text(serialized);

                            for &token in tokens.iter() {
                                // @TODO check whether connection still exists
                                let conn = &mut connections[token.into()];
                                info!("[WORKER {}] sending msg {:?}", worker_index, msg);

                                conn.send_message(msg.clone())
                                    .expect("failed to send message");

                                poll.reregister(
                                    conn.socket(),
                                    conn.token(),
                                    conn.events(),
                                    PollOpt::edge() | PollOpt::oneshot(),
                                ).unwrap();
                            }
                        }
                    }
                });
            });
    }
}
