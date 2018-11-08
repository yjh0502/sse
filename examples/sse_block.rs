extern crate env_logger;
extern crate futures;
extern crate hyper;
extern crate rand;
extern crate sse;
extern crate tokio;
extern crate tokio_timer;

use std::time::*;

use futures::future::*;
use futures::*;
use rand::Rng;

use sse::*;

fn server(addr: &str) -> Box<Future<Item = (), Error = ()> + Send> {
    let addr = addr.parse().expect("addres parsing failed");

    let (serv, sender) = EventService::pair_sse(BroadcastFlags::empty());

    let f_server = hyper::server::Server::bind(&addr)
        .serve(move || Ok::<_, hyper::Error>(serv.clone()))
        .map(|_| ())
        .map_err(|e| eprintln!("failed to serve: {:?}", e));

    let timer_interval = Duration::from_millis(100);
    let mut event_counter = 0;

    let mut rng = rand::thread_rng();
    let body = rng.gen_ascii_chars().take(1024 * 16).collect::<String>();

    let stream_send = tokio_timer::Interval::new(Instant::now(), timer_interval)
        .map_err(|_e| -> () { panic!("timer error") })
        .fold(sender, move |sender, _to| {
            event_counter += 1;
            let msg = BroadcastMessage::new("tick", body.clone());
            sender.send(BroadcastEvent::Message(msg)).map_err(|_e| ())
        }).map(|_s| eprintln!("tick end"));

    let f = f_server.select(stream_send).then(|_| Ok(()));
    Box::new(f)
}

fn main() {
    env_logger::init();
    let addr = "127.0.0.1:18080";

    tokio::run(server(addr));
}
