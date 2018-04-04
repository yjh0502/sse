extern crate env_logger;
extern crate futures;
extern crate hyper;
extern crate rand;
extern crate sse;
extern crate tokio_core;
extern crate tokio_timer;

use std::rc::*;
use std::time::Duration;

use futures::future::*;
use futures::*;
use hyper::server::Http;
use rand::Rng;
use tokio_core::reactor::*;

use sse::*;

fn server(addr: &str, handle: Handle) -> Box<Future<Item = (), Error = ()>> {
    let addr = addr.parse().expect("addres parsing failed");

    let (serv, sender) = EventService::pair2(&handle, BroadcastFlags::empty());
    let serv = Rc::new(serv);

    let serve = Http::new()
        .max_buf_size(1024 * 16)
        .serve_addr_handle(&addr, &handle, move || Ok(serv.clone()))
        .expect("unable to create server");

    eprintln!("listen: {}", serve.incoming_ref().local_addr());

    let timer = tokio_timer::wheel()
        .tick_duration(Duration::from_millis(10))
        .build();

    let timer_interval = Duration::from_millis(100);
    let mut event_counter = 0;

    let mut rng = rand::thread_rng();
    let body = rng.gen_ascii_chars().take(1024 * 16).collect::<String>();

    let stream_send = timer
        .interval(timer_interval)
        .map_err(|_e| -> () { panic!("timer error") })
        .fold(sender, move |sender, _to| {
            event_counter += 1;
            let msg = BroadcastMessage::new("tick", body.clone());
            sender.send(BroadcastEvent::Message(msg)).map_err(|_e| ())
        })
        .map(|_s| -> () { panic!("tick end") });

    let f_listen = serve
        .for_each(move |conn| {
            handle.spawn(
                conn.map(|_| ())
                    .map_err(|err| eprintln!("serve error: {:?}", err)),
            );
            Ok(())
        })
        .into_future()
        .map_err(|_e| {
            eprintln!("failed to listen: {:?}", _e);
        });

    let f = stream_send.select(f_listen).then(|_| {
        eprintln!("server exit");
        ok::<(), ()>(())
    });
    Box::new(f)
}

fn main() {
    env_logger::init();
    let addr = "127.0.0.1:18080";

    let mut core = tokio_core::reactor::Core::new().expect("failed to build core");
    let handle = core.handle();

    let f_server = server(addr, handle.clone());

    core.run(f_server).unwrap();

    /*
    let client_url_str = format!("http://{}/", addr);
    let client_url = client_url_str
        .parse::<hyper::Uri>()
        .expect("failed to parse uri");

    let client = SSEStream::new(client_url, &handle);
    let timer = tokio_timer::Timer::default();
    let f_client = client
        .map_err(|_e| -> () { panic!("error on client: {:?}", _e) })
        .fold((), move |s, msg| {
            timer.sleep(Duration::from_millis(1000)).then(move |_| {
                eprintln!("msg: {:?}", msg.id);
                ok::<_, ()>(s)
            })
        });

    core.run(f_server.join(f_client))
        .expect("unable to run server");
        */
}
