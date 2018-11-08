use super::*;

#[derive(Debug)]
pub enum BroadcastRawEvent {
    NewClient(Client),
    Message(Bytes),
}

type BroadcastRawFuture = Box<Future<Item = BroadcastRaw, Error = ()> + Send>;
pub struct BroadcastRaw {
    clients: Vec<Client>,
}

impl BroadcastRaw {
    pub fn new() -> Self {
        Self {
            clients: Vec::new(),
        }
    }

    fn on_client(mut self, client: Client) -> BroadcastRawFuture {
        self.clients.push(client);
        Box::new(ok(self))
    }

    fn on_ephimeral_msg(mut self, bytes: Bytes) -> BroadcastRawFuture {
        let mut clients = Vec::new();
        std::mem::swap(&mut self.clients, &mut clients);

        let mut tx = Vec::with_capacity(clients.len());
        for mut c in clients {
            tx.push((c, vec![hyper::Chunk::from(bytes.clone())]));
        }

        self.on_flush(Vec::new(), tx)
    }

    fn on_flush(
        mut self,
        mut clients: Vec<Client>,
        tx: Vec<(Client, Vec<hyper::Chunk>)>,
    ) -> BroadcastRawFuture {
        let tx_iter = tx.into_iter().map(move |(c, msgs)| {
            let f = c
                .sender
                .clone()
                .send_all(iter_ok(msgs))
                .map_err(|_e| {
                    // send error. fired when client leaves
                }).map(move |_sender| c);

            tokio_timer::Timeout::new(f, Duration::from_millis(FLUSH_DEADLINE_MS)).map_err(|_e| {
                // send timeout. actual timeout will happens when hyper internal buffer and TCP
                // send buffer is both full.
                ()
            })
        });

        let f = futures_unordered(tx_iter)
            .map(Some)
            .or_else(|_e: ()| Ok::<_, ()>(None))
            .filter_map(|x| x)
            .collect()
            .and_then(move |mut tx_clients| {
                clients.append(&mut tx_clients);
                std::mem::swap(&mut self.clients, &mut clients);
                ok(self)
            });

        Box::new(f)
    }

    pub fn on_event(self, ev: BroadcastRawEvent) -> BroadcastRawFuture {
        use self::BroadcastRawEvent::*;
        match ev {
            NewClient(client) => self.on_client(client),
            Message(msg) => self.on_ephimeral_msg(msg),
        }
    }
}
