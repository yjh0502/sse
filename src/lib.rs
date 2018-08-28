#[macro_use]
extern crate bitflags;
extern crate bytes;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate futures;
extern crate hyper;
#[macro_use]
extern crate log;
extern crate http;
extern crate tokio;
extern crate tokio_timer;

use std::time::*;

use bytes::*;
use futures::future::*;
use futures::stream::*;
use futures::sync::mpsc;
use futures::*;
use futures::{Future, Sink, Stream};
use hyper::header::*;
use hyper::service::Service;
use hyper::{Body, Chunk, Method, Request, Response, StatusCode};

const FLUSH_DEADLINE_MS: u64 = 200;

mod parse;

mod raw;
pub use raw::*;
mod sse;
pub use sse::*;

mod client;
pub use client::*;

pub mod error {
    use super::*;

    error_chain! {
        foreign_links {
            Hyper(hyper::Error);
            Http(http::Error);
        }

        errors {
            InvalidUTF8(e: std::str::Utf8Error) {
                description("invalid UTF8")
            }
            Protocol(e: parse::ParseError) {
                description("protocol")
            }
        }
    }
}

#[derive(Debug)]
struct RecvError;
impl std::fmt::Display for RecvError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(fmt, "recv error")
    }
}
impl std::error::Error for RecvError {}

type HttpSender = mpsc::Sender<Chunk>;
pub static LAST_EVENT_ID: &[u8] = b"last-event-id";

fn get_event_id(req: &Request<Body>) -> Option<usize> {
    let name = HeaderName::from_lowercase(LAST_EVENT_ID).ok()?;
    let last_event_id = req.headers().get(name)?;
    let last_event_id = std::str::from_utf8(last_event_id.as_bytes()).ok()?;
    let event_id = last_event_id.parse::<usize>().ok()?;
    Some(event_id)
}

#[derive(Debug)]
pub struct Client {
    req: Request<Body>,
    sender: HttpSender,
    seq: usize,
}
impl Client {
    pub fn new(req: Request<Body>, sender: HttpSender) -> Self {
        let mut seq = 0;
        if let Some(event_id) = get_event_id(&req) {
            seq = event_id + 1;
        }

        Self { req, sender, seq }
    }
}

fn hyper_resp_err() -> Response<Body> {
    Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(Body::empty())
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

#[derive(Clone)]
pub struct EchoService {
    sender: mpsc::Sender<BroadcastRawEvent>,
    inner: EventService,
}

impl EchoService {
    pub fn new() -> Self {
        let (inner, sender) = EventService::pair_raw();
        Self { sender, inner }
    }
}

impl Service for EchoService {
    type ReqBody = Body;
    type ResBody = Body;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Response<Body>, Error = Self::Error>>;

    fn call(&mut self, req: Request<Self::ReqBody>) -> Self::Future {
        let method = req.method().clone();
        match method {
            Method::GET => {
                return self.inner.call(req);
            }

            Method::POST => {
                let body = req.into_body();

                let sender = self.sender.clone();
                let f =
                    body.map_err(|_e| {
                        error!("failed to read body: {:?}", _e);
                    }).fold(sender, |sender, chunk| {
                            let bytes: Bytes = chunk.to_vec().into();
                            sender
                                .send(BroadcastRawEvent::Message(bytes))
                                .map_err(|_e| {
                                    error!("failed to send: {:?}", _e);
                                })
                        })
                        .map_err(|_e| {
                            error!("failed to send body: {:?}", _e);
                        })
                        .then(|_| Ok(hyper_resp_err()));

                Box::new(f)
            }

            _ => Box::new(ok(hyper_resp_err())),
        }
    }
}

#[derive(Clone)]
pub struct EventService {
    sender: mpsc::Sender<(Request<Body>, HttpSender)>,
}

impl EventService {
    /// (service, connection receiver) pair
    pub fn pair() -> (Self, mpsc::Receiver<(Request<Body>, HttpSender)>) {
        let (sender, receiver) = mpsc::channel(10);
        let serv = Self { sender };
        (serv, receiver)
    }

    pub fn pair_raw() -> (Self, mpsc::Sender<BroadcastRawEvent>) {
        let (serv, conn_receiver) = EventService::pair();
        let (sender, receiver) = mpsc::channel(10);

        let stream_rx = conn_receiver
            .map(|(req, conn)| {
                //
                BroadcastRawEvent::NewClient(Client::new(req, conn))
            })
            .map_err(|_e| {
                error!("error on accepting connections: {:?}", _e);
                ()
            });

        let broadcast = BroadcastRaw::new();
        let broker = receiver
            .select(stream_rx)
            .fold(broadcast, BroadcastRaw::on_event)
            .map(|_b| ());

        tokio::spawn(broker);

        (serv, sender)
    }

    /// (service, msg sender) pair
    pub fn pair_sse(opt: BroadcastFlags) -> (Self, mpsc::Sender<BroadcastEvent>) {
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

        tokio::spawn(broker);

        (serv, sender)
    }
}

impl Service for EventService {
    type ReqBody = Body;
    type ResBody = Body;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Response<Body>, Error = Self::Error> + Send>;

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let (sender, receiver) = mpsc::channel(16);
        let receiver: Box<Stream<Item = _, Error = _> + Send + 'static> =
            Box::new(receiver.map_err(|_e| {
                let e: Box<std::error::Error + Send + Sync> = Box::new(RecvError);
                e
            }));
        let body = Body::from(receiver);

        let msg = (req, sender);
        let f = self.sender.clone().send(msg).then(|res| match res {
            Ok(_) => Ok(Response::builder()
                .status(hyper::StatusCode::OK)
                .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                .header(CONTENT_TYPE, "text/event-stream")
                .body(body)
                .unwrap_or_else(|_| Response::new(Body::empty()))),
            Err(_e) => {
                // failed to register client to SSE worker
                Ok(hyper_resp_err())
            }
        });
        Box::new(f)
    }
}
