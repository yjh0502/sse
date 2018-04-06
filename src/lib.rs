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

use std::time::Duration;

use bytes::*;
use futures::future::*;
use futures::stream::*;
use futures::sync::mpsc;
use futures::*;
use futures::{Future, Sink, Stream};
use hyper::header::Headers;
use hyper::header::{AccessControlAllowOrigin, Connection, ContentType};
use hyper::server::{Request, Response, Service};
use hyper::{mime, Chunk, StatusCode};
use tokio_core::reactor::*;

header! { (LastEventId, "Last-Event-ID") => [String] }

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
        }
    }
}

type HttpMsg = Result<Chunk, hyper::Error>;
type HttpSender = mpsc::Sender<HttpMsg>;

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

#[derive(Clone)]
pub struct EchoService {
    handle: Handle,
    sender: mpsc::Sender<BroadcastRawEvent>,
    inner: std::rc::Rc<EventService>,
}

impl EchoService {
    pub fn new(handle: Handle) -> Self {
        let (inner, sender) = EventService::pair_raw(&handle);
        Self {
            handle,
            sender,
            inner: std::rc::Rc::new(inner),
        }
    }
}

impl Service for EchoService {
    type Request = hyper::Request;
    type Response = hyper::Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        use hyper::Method::*;

        let method = req.method().clone();

        match method {
            Get => {
                let inner = self.inner.clone();
                return inner.call(req);
            }

            Post => {
                let body = req.body();

                let sender = self.sender.clone();
                let f = body.map_err(|_e| {
                    eprintln!("failed to read body: {:?}", _e);
                }).fold(sender, |sender, chunk| {
                        let bytes: Bytes = chunk.to_vec().into();
                        sender
                            .send(BroadcastRawEvent::Message(bytes))
                            .map_err(|_e| {
                                eprintln!("failed to send: {:?}", _e);
                            })
                    })
                    .map_err(|_e| hyper::error::Error::Method)
                    .then(|_| {
                        Ok(Response::new()
                            .with_status(StatusCode::ServiceUnavailable)
                            .with_header(AccessControlAllowOrigin::Any))
                    });

                Box::new(f)
            }

            _ => {
                let resp = Response::new()
                    .with_status(StatusCode::ServiceUnavailable)
                    .with_header(AccessControlAllowOrigin::Any);
                Box::new(ok(resp))
            }
        }
    }
}

#[derive(Clone)]
pub struct EventService {
    headers: Headers,
    sender: mpsc::Sender<(Request, HttpSender)>,
}

impl EventService {
    /// (service, connection receiver) pair
    pub fn pair(headers: Headers) -> (Self, mpsc::Receiver<(Request, HttpSender)>) {
        let (sender, receiver) = mpsc::channel(10);
        let serv = Self { headers, sender };
        (serv, receiver)
    }

    pub fn pair_raw(handle: &Handle) -> (Self, mpsc::Sender<BroadcastRawEvent>) {
        let headers = Headers::new();

        let (serv, conn_receiver) = EventService::pair(headers);
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

        handle.spawn(broker);

        (serv, sender)
    }

    /// (service, msg sender) pair
    pub fn pair_sse(handle: &Handle, opt: BroadcastFlags) -> (Self, mpsc::Sender<BroadcastEvent>) {
        let headers = {
            let mut headers = Headers::new();
            headers.set(ContentType(mime::TEXT_EVENT_STREAM));
            headers
        };

        let (serv, conn_receiver) = EventService::pair(headers);
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

        let mut headers = self.headers.clone();
        headers.set(AccessControlAllowOrigin::Any);
        headers.set(Connection::keep_alive());

        let msg = (req, sender);
        let f = self.sender.clone().send(msg).then(|res| match res {
            Ok(_) => Ok(Response::new()
                .with_status(StatusCode::Ok)
                .with_headers(headers)
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
