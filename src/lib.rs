extern crate bytes;
#[macro_use]
extern crate futures;
extern crate hyper;
extern crate tokio_core;
extern crate tokio_timer;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate log;
#[macro_use]
extern crate bitflags;

// server sent events
use std::io::Write;

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
type ConnSender = mpsc::Sender<HttpSender>;

#[derive(Clone)]
pub struct EventService {
    sender: ConnSender,
}

impl EventService {
    /// (service, connection receiver) pair
    pub fn pair() -> (Self, mpsc::Receiver<HttpSender>) {
        let (sender, receiver) = mpsc::channel(10);
        let serv = Self { sender };
        (serv, receiver)
    }

    /// (service, msg sender) pair
    pub fn pair2(handle: Handle, opt: BroadcastFlags) -> (Self, mpsc::Sender<BroadcastEvent>) {
        let (serv, conn_receiver) = EventService::pair();
        let (sender, receiver) = mpsc::channel(10);

        let stream_rx = conn_receiver
            .map(|conn| BroadcastEvent::NewClient(Client::new(conn)))
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

    fn call(&self, _req: Request) -> Self::Future {
        info!("request events");
        let (tx_msg, rx_msg) = mpsc::channel(10);
        let f = self.sender
            .clone()
            .send(tx_msg)
            .map_err(|_| hyper::Error::Incomplete)
            .and_then(|_| {
                Ok(Response::new()
                    .with_status(StatusCode::Ok)
                    .with_header(AccessControlAllowOrigin::Any)
                    .with_header(ContentType(mime::TEXT_EVENT_STREAM))
                    .with_header(Connection::keep_alive())
                    .with_body(rx_msg))
            });
        Box::new(f)
    }
}

#[derive(Debug)]
pub struct Client {
    sender: HttpSender,
    seq: usize,
}
impl Client {
    pub fn new(sender: HttpSender) -> Self {
        Self { sender, seq: 0 }
    }
}

#[derive(Debug, Clone)]
pub struct BroadcastMessage {
    inner: Bytes,
}
impl BroadcastMessage {
    /// `data` should not contain newline...
    pub fn new(event: &str, data: String) -> Self {
        let mut buf = BytesMut::with_capacity(512).writer();
        write!(buf, "event: {}\ndata: {}\n\n", event, data).expect("msg write failed");

        let inner: Bytes = buf.into_inner().freeze();

        Self { inner }
    }
}

#[derive(Debug)]
pub enum BroadcastEvent {
    NewClient(Client),
    Message(BroadcastMessage),
    EphimeralMessage(BroadcastMessage),

    Reset,

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
    messages: Vec<BroadcastMessage>,
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
        if self.opt.contains(BroadcastFlags::NO_LOG) {
            client.seq = self.messages.len();
        }

        let mut clients = Vec::new();
        std::mem::swap(&mut self.clients, &mut clients);

        let seq = client.seq;
        let tx = vec![(client, self.messages[seq..].to_vec())];
        self.on_flush(clients, tx)
    }

    fn on_msg(mut self, msg: BroadcastMessage) -> BroadcastFuture {
        let mut clients = Vec::new();
        std::mem::swap(&mut self.clients, &mut clients);

        self.messages.push(msg);
        // seq for incoming message
        let seq = self.messages.len();

        // bypass borrow checker
        let mut tx = Vec::with_capacity(clients.len());
        for mut c in clients.into_iter() {
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
        for mut c in clients.into_iter() {
            tx.push((c, vec![msg.clone()]));
        }
        self.on_flush(Vec::new(), tx)
    }

    fn on_flush(
        mut self,
        mut clients: Vec<Client>,
        tx: Vec<(Client, Vec<BroadcastMessage>)>,
    ) -> BroadcastFuture {
        let tx_iter = tx.into_iter().map(|(c, msgs)| {
            let sender = c.sender.clone();
            iter_ok(msgs)
                .fold(sender, |sender, msg| {
                    sender.send(Ok(Chunk::from(msg.inner.clone())))
                })
                .map(move |_sender| c)
        });

        let f = futures_unordered(tx_iter)
            .map(|x| Some(x))
            .or_else(|e: mpsc::SendError<HttpMsg>| {
                trace!("{:?} client removed", e);
                Ok::<_, ()>(None)
            })
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

    pub fn on_event(self, ev: BroadcastEvent) -> BroadcastFuture {
        use self::BroadcastEvent::*;
        match ev {
            Message(msg) => self.on_msg(msg),
            EphimeralMessage(msg) => self.on_ephimeral_msg(msg),
            NewClient(client) => self.on_client(client),
            Reset => self.on_reset(),

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

// client
fn parse_sse_chunk<'a>(s: &'a str) -> Option<(String, String)> {
    let lines = s.split("\n");

    let mut event = String::new();
    let mut data = String::new();

    for line in lines {
        if line.is_empty() {
            continue;
        }

        let mut tup = line.splitn(2, ": ");
        let first = tup.next()?;
        let second = tup.next()?;

        if first == "event" {
            event = second.to_owned();
        } else {
            data += second;
        }
    }

    Some((event, data))
}

struct SSEBodyStream {
    body: hyper::Body,
}
impl SSEBodyStream {
    fn new(body: hyper::Body) -> Self {
        Self { body }
    }
}

impl Stream for SSEBodyStream {
    type Item = (String, String);
    type Error = error::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        match try_ready!(self.body.poll()) {
            None => Ok(Async::Ready(None)),
            Some(chunk) => {
                if let Ok(chunk_str) = std::str::from_utf8(&chunk) {
                    if let Some(tup) = parse_sse_chunk(chunk_str) {
                        return Ok(Async::Ready(Some(tup)));
                    }
                }
                bail!("invalid chunk: {:?}", String::from_utf8_lossy(&chunk));
            }
        }
    }
}

pub struct SSEStream {
    url: hyper::Uri,
    client: hyper::Client<hyper::client::HttpConnector>,
    timer: tokio_timer::Timer,

    fut_req: Option<Box<Future<Item = hyper::Response, Error = hyper::Error>>>,
    inner: Option<SSEBodyStream>,
}

impl SSEStream {
    pub fn new(url: hyper::Uri, handle: &Handle) -> Self {
        let client = hyper::Client::new(&handle);
        let timer = tokio_timer::Timer::default();
        Self {
            url,
            client,
            timer,

            fut_req: None,
            inner: None,
        }
    }
}

impl Stream for SSEStream {
    type Item = (String, String);
    type Error = error::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        if let Some(mut fut_req) = self.fut_req.take() {
            match fut_req.poll() {
                Err(_e) => {
                    error!("failed to connect, retry: {:?}", _e);
                    // fallthrough
                }
                Ok(Async::NotReady) => {
                    self.fut_req = Some(fut_req);
                    return Ok(Async::NotReady);
                }
                Ok(Async::Ready(resp)) => {
                    info!("sse stream connected: {}", self.url);
                    self.inner = Some(SSEBodyStream::new(resp.body()));
                }
            }
        }

        if let Some(ref mut s) = self.inner {
            match s.poll() {
                Err(_e) => {
                    error!("failed to read body, trying to reconnect: {:?}", _e);
                    // fallthrough
                }
                Ok(res) => return Ok(res),
            }
        }

        // retry case
        self.inner = None;
        info!("trying to connect: {}", self.url);
        let req = Request::new(hyper::Method::Get, self.url.clone());
        let client = self.client.clone();
        let req = self.timer
            .sleep(std::time::Duration::from_millis(100))
            .then(move |_| client.request(req));

        self.fut_req = Some(Box::new(req));
        self.poll()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_chunk() {
        let s = r#"event: foo
data: test
"#;

        assert_eq!(
            Some(("foo".to_owned(), "test".to_owned())),
            parse_sse_chunk(s)
        );
    }

    #[test]
    fn test_chunk_multiline() {
        let s = r#"event: foo
data: {
data: "foo":"bar"
data: }
"#;

        assert_eq!(
            Some(("foo".to_owned(), r#"{"foo":"bar"}"#.to_owned())),
            parse_sse_chunk(s)
        );
    }
}
