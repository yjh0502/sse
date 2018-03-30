extern crate bytes;
#[macro_use]
extern crate futures;
#[macro_use]
extern crate hyper;
extern crate tokio_core;
extern crate tokio_timer;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
#[macro_use]
extern crate bitflags;

use bytes::*;
use futures::*;
use futures::future::*;
use futures::{Future, Sink, Stream};
use futures::stream::*;
use futures::sync::mpsc;
use hyper::Chunk;
use hyper::header::{AccessControlAllowOrigin, Connection, ContentType};
use hyper::mime;
use hyper::server::{Request, Response, Service};
use hyper::StatusCode;
use tokio_core::reactor::*;

header! { (LastEventId, "Last-Event-ID") => [String] }

mod parse;
mod client;
pub use client::*;

pub mod error {
    use super::*;

    error_chain! {
        foreign_links {
            Hyper(hyper::Error);
        }
    }
}

type HttpMsg = Result<Chunk, hyper::Error>;
type HttpSender = mpsc::Sender<HttpMsg>;

#[derive(Clone)]
pub struct EventService {
    sender: mpsc::Sender<(Request, HttpSender)>,
}

impl EventService {
    /// (service, connection receiver) pair
    pub fn pair() -> (Self, mpsc::Receiver<(Request, HttpSender)>) {
        let (sender, receiver) = mpsc::channel(10);
        let serv = Self { sender };
        (serv, receiver)
    }

    /// (service, msg sender) pair
    pub fn pair2(handle: &Handle, opt: BroadcastFlags) -> (Self, mpsc::Sender<BroadcastEvent>) {
        let (serv, conn_receiver) = EventService::pair();
        let (sender, receiver) = mpsc::channel(10);

        let stream_rx = conn_receiver
            .map(|(req, conn)| {
                //
                BroadcastEvent::NewClient(Client::new(req, conn))
            })
            .map_err(|_e| {
                error!("error on accepting connections: {:?}", _e);
                ()
            });

        let broadcast = Broadcast::new(opt);
        let broker = receiver
            .select(stream_rx)
            .fold(broadcast, Broadcast::on_event)
            .map(|_b| ());

        handle.spawn(broker);

        (serv, sender)
    }
}

impl Service for EventService {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        let (sender, body) = hyper::Body::pair();

        let msg = (req, sender);
        let f = self.sender.clone().send(msg).then(|res| match res {
            Ok(_) => Ok(Response::new()
                .with_status(StatusCode::Ok)
                .with_header(AccessControlAllowOrigin::Any)
                .with_header(ContentType(mime::TEXT_EVENT_STREAM))
                .with_header(Connection::keep_alive())
                .with_body(body)),
            Err(_e) => {
                // failed to register client to SSE worker
                Ok(Response::new()
                    .with_status(StatusCode::ServiceUnavailable)
                    .with_header(AccessControlAllowOrigin::Any))
            }
        });
        Box::new(f)
    }
}

#[derive(Debug)]
pub struct Client {
    req: Request,
    sender: HttpSender,
    seq: usize,
}
impl Client {
    pub fn new(req: Request, sender: HttpSender) -> Self {
        let mut seq = 0;
        if let Some(last_event_id) = req.headers().get::<LastEventId>() {
            if let Ok(event_id) = last_event_id.parse::<usize>() {
                seq = event_id + 1;
            }
        }

        Self { req, sender, seq }
    }
}

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

    //TODO: reduce allocs
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
    opt: BroadcastFlags,
    clients: Vec<Client>,
    messages: Vec<Bytes>,
}

impl Broadcast {
    pub fn new(opt: BroadcastFlags) -> Self {
        Self {
            opt,
            clients: Vec::new(),
            messages: Vec::new(),
        }
    }

    fn on_client(mut self, mut client: Client) -> BroadcastFuture {
        trace!("client {} registered", self.clients.len());

        // TODO: move to somewhere else?
        let mut seq = client.seq;

        // handle invalid LastEventId
        if seq >= self.messages.len() || self.opt.contains(BroadcastFlags::NO_LOG) {
            seq = self.messages.len();
        }

        let mut clients = Vec::new();
        std::mem::swap(&mut self.clients, &mut clients);

        client.seq = self.messages.len();
        let tx = vec![(client, self.messages[seq..].to_vec())];
        self.on_flush(clients, tx)
    }

    fn on_msg(mut self, mut msg: BroadcastMessage) -> BroadcastFuture {
        let mut clients = Vec::new();
        std::mem::swap(&mut self.clients, &mut clients);

        let seq = self.messages.len();
        msg.event_id = Some(seq);

        self.messages.push(msg.to_bytes());
        // seq for incoming message
        let seq = self.messages.len();

        // bypass borrow checker
        let mut tx = Vec::with_capacity(clients.len());
        for mut c in clients {
            // clone pending messages
            let msgs = self.messages[c.seq..seq].to_vec();
            c.seq = seq;
            tx.push((c, msgs));
        }
        self.on_flush(Vec::new(), tx)
    }

    fn on_ephimeral_msg(mut self, msg: BroadcastMessage) -> BroadcastFuture {
        let mut clients = Vec::new();
        std::mem::swap(&mut self.clients, &mut clients);

        let mut tx = Vec::with_capacity(clients.len());
        for mut c in clients {
            tx.push((c, vec![msg.to_bytes()]));
        }

        self.on_flush(Vec::new(), tx)
    }

    fn on_flush(
        mut self,
        mut clients: Vec<Client>,
        tx: Vec<(Client, Vec<Bytes>)>,
    ) -> BroadcastFuture {
        let tx_iter = tx.into_iter().map(|(c, msgs)| {
            let sender = c.sender.clone();

            let msgs = iter_ok(msgs.into_iter().map(|msg| Ok(msg.into())));
            sender.send_all(msgs).map(move |_sender| c)
        });

        let f = futures_unordered(tx_iter)
            .map(Some)
            .or_else(|_e: mpsc::SendError<HttpMsg>| Ok::<_, ()>(None))
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
