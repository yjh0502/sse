use super::*;

#[derive(Debug, Clone)]
pub struct BroadcastMessage {
    event_id: Option<usize>,
    event: String,
    data: String,
    inner: Bytes,
}

impl BroadcastMessage {
    /// `data` should not contain newline...
    pub fn new(event: &str, data: String) -> Self {
        //let s = format!("event: {}\ndata: {}\n\n", event, data);
        Self {
            event: event.to_owned(),
            data,
            event_id: None,
            inner: Bytes::new(),
        }
    }

    fn to_bytes(&self) -> Bytes {
        let mut s = String::new();
        if let Some(id) = self.event_id {
            s += &format!("id: {}\n", id);
        }
        s += &format!("event: {}\ndata: {}\n\n", self.event, self.data);
        Bytes::from(s)
    }
}

#[derive(Debug)]
pub enum BroadcastEvent {
    NewClient(Client),

    Ping,
    Message(BroadcastMessage),
    EphimeralMessage(BroadcastMessage),

    Reset,
    DebugDisconnect,

    Inspect(unsync::oneshot::Sender<usize>),
}

bitflags! {
    pub struct BroadcastFlags: u8 {
        const NO_LOG = 0x1;
    }
}

type BroadcastFuture = Box<Future<Item = Broadcast, Error = ()>>;
pub struct Broadcast {
    timer: tokio_timer::Timer,

    opt: BroadcastFlags,
    clients: Vec<Client>,

    messages: Vec<Bytes>,
    message_offset: usize,
}

impl Broadcast {
    pub fn new(opt: BroadcastFlags) -> Self {
        let timer = tokio_timer::wheel()
            .tick_duration(Duration::from_millis(10))
            .build();

        Self {
            timer,

            opt,
            clients: Vec::new(),

            messages: Vec::new(),
            message_offset: 0,
        }
    }

    fn next_event_id(&self) -> usize {
        self.message_offset + self.messages.len()
    }

    fn on_client(mut self, client: Client) -> BroadcastFuture {
        trace!("client {} registered", self.clients.len());

        // TODO: move to somewhere else?
        let mut seq = client.seq;

        // handle invalid LastEventId
        let next_event_id = self.next_event_id();
        if seq >= next_event_id || self.opt.contains(BroadcastFlags::NO_LOG) {
            seq = next_event_id;
        }
        if seq < self.message_offset {
            seq = self.message_offset;
        }

        let mut clients = Vec::new();
        std::mem::swap(&mut self.clients, &mut clients);

        // send pending messages
        let vec_offset = seq - self.message_offset;
        let chunks = self.messages[vec_offset..]
            .iter()
            .map(|s| Ok(hyper::Chunk::from(s.clone())))
            .collect::<Vec<_>>();

        let tx = vec![(client, chunks)];
        self.on_flush(clients, tx)
    }

    fn on_ping(self) -> BroadcastFuture {
        let bytes = Bytes::from(":ping\n\n");
        self.broadcast_bytes(bytes)
    }

    fn on_msg(mut self, mut msg: BroadcastMessage) -> BroadcastFuture {
        msg.event_id = Some(self.next_event_id());
        let bytes = msg.to_bytes();
        self.messages.push(bytes.clone());

        self.broadcast_bytes(msg.to_bytes())
    }

    fn on_ephimeral_msg(self, msg: BroadcastMessage) -> BroadcastFuture {
        self.broadcast_bytes(msg.to_bytes())
    }

    fn broadcast_bytes(mut self, bytes: Bytes) -> BroadcastFuture {
        let mut clients = Vec::new();
        std::mem::swap(&mut self.clients, &mut clients);

        let mut tx = Vec::with_capacity(clients.len());
        for mut c in clients {
            tx.push((c, vec![Ok(hyper::Chunk::from(bytes.clone()))]));
        }
        self.on_flush(Vec::new(), tx)
    }

    fn on_flush(
        mut self,
        mut clients: Vec<Client>,
        tx: Vec<(Client, Vec<Result<hyper::Chunk, hyper::Error>>)>,
    ) -> BroadcastFuture {
        let timer = self.timer.clone();
        let tx_iter = tx.into_iter().map(move |(c, msgs)| {
            let f = c.sender
                .clone()
                .send_all(iter_ok(msgs))
                .map_err(|_e| {
                    // send error. fired when client leaves
                })
                .map(move |_sender| c);

            timer
                .timeout(f, Duration::from_millis(FLUSH_DEADLINE_MS))
                .map_err(|_e| {
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

    fn on_reset(mut self) -> BroadcastFuture {
        // drop all connections and message
        // TODO: check if client can receive all messages before disconnecting
        self.clients.clear();

        self.message_offset += self.messages.len();
        self.messages.clear();
        Box::new(ok(self))
    }

    fn on_debug_disconnect(mut self) -> BroadcastFuture {
        self.clients.clear();
        Box::new(ok(self))
    }

    pub fn on_event(self, ev: BroadcastEvent) -> BroadcastFuture {
        use self::BroadcastEvent::*;
        match ev {
            Ping => self.on_ping(),
            Message(msg) => self.on_msg(msg),
            EphimeralMessage(msg) => self.on_ephimeral_msg(msg),
            NewClient(client) => self.on_client(client),
            Reset => self.on_reset(),
            DebugDisconnect => self.on_debug_disconnect(),

            Inspect(sender) => {
                let len = self.clients.len();
                if let Err(_e) = sender.send(len) {
                    error!("failed to report stat: {:?}", _e);
                }
                Box::new(ok(self))
            }
        }
    }
}
