extern crate env_logger;
#[macro_use]
extern crate log;
#[macro_use]
extern crate error_chain;
extern crate futures;
extern crate hyper;
extern crate structopt;
#[macro_use]
extern crate structopt_derive;
extern crate rand;
extern crate sse;
extern crate tokio;
extern crate tokio_timer;

use std::sync::{atomic, Arc};
use std::time::*;

use futures::future::*;
use futures::*;
use hyper::{Body, Request};
use rand::Rng;

use sse::*;

pub mod error {
    use super::*;

    error_chain! {
        foreign_links {
            Hyper(hyper::Error);
        }
    }
}

use error::*;

struct NullExecutor;
impl<F> Executor<F> for NullExecutor
where
    F: Future<Item = (), Error = ()>,
{
    fn execute(&self, _f: F) -> std::result::Result<(), ExecuteError<F>> {
        unimplemented!();
    }
}

#[derive(Clone)]
struct Counter {
    count: Arc<atomic::AtomicUsize>,
}

fn spawn_client(
    client: &hyper::Client<hyper::client::HttpConnector>,
    req: Request<Body>,
    counter: Counter,
) -> impl Future<Item = (), Error = ()> {
    let f = client
        .request(req)
        .map_err(|e| -> () {
            panic!("failed to connect: {:?}", e);
        })
        .and_then(move |res| {
            res.into_body()
                .map_err(|e| -> () {
                    panic!("failed to read body: {:?}", e);
                })
                .fold(counter, |counter, _chunk| {
                    counter.count.fetch_add(1, atomic::Ordering::SeqCst);
                    ok::<Counter, ()>(counter)
                })
        })
        .map(|_counter| ());

    f
}

fn run_client(
    client: hyper::Client<hyper::client::HttpConnector>,
    url: hyper::Uri,
    count: usize,
    counter: Counter,
) -> Box<Future<Item = (), Error = Error> + Send> {
    let clients = (0..count)
        .map(|_n| {
            let req = Request::builder()
                .method(hyper::Method::GET)
                .uri(url.clone())
                .body(Body::empty())
                .unwrap();
            spawn_client(&client, req, counter.clone())
        })
        .collect::<Vec<_>>();

    let f = join_all(clients)
        .map(|_| ())
        .or_else(|_e| bail!("error on client"));
    Box::new(f)
}

fn run_server(opt: &Opt) -> Box<Future<Item = (), Error = Error> + Send> {
    let addr_str = format!("0.0.0.0:{}", opt.port);
    let addr = addr_str.parse().expect("addres parsing failed");

    let (serv, sender) = EventService::pair_sse(BroadcastFlags::empty());

    let f_server = hyper::server::Server::bind(&addr)
        .serve(move || Ok::<_, hyper::Error>(serv.clone()))
        .map(|_| ())
        .map_err(|e| eprintln!("failed to serve: {:?}", e));

    let timer_interval = Duration::from_millis(1000);
    let mut event_counter = 0;

    let mut rng = rand::thread_rng();
    let body = rng.gen_ascii_chars().take(64).collect::<String>();

    let stream_send = tokio_timer::Interval::new(Instant::now(), timer_interval)
        .map_err(|_e| -> () { panic!("timer error") })
        .fold(sender, move |sender, _to| {
            event_counter += 1;
            let msg = BroadcastMessage::new("tick", body.clone());
            sender.send(BroadcastEvent::Message(msg)).map_err(|_e| ())
        })
        .map(|_s| -> () { panic!("tick end") });

    let f = stream_send.select(f_server).then(|_| {
        error!("server exit");
        ok::<(), _>(())
    });
    Box::new(f)
}

fn rand_localhost() -> String {
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let a = 127;
    let b = 0;
    let c: u8 = rng.gen_range(0, 254);
    let d: u8 = rng.gen_range(0, 254);
    format!("{}.{}.{}.{}", a, b, c, d)
}

fn run(opt: &Opt) -> Result<()> {
    // let mut core = Core::new().expect("failed to build core");

    let duration_timeout = Duration::from_secs(opt.duration as u64);

    let num_threads = opt.threads;
    let counter = Counter {
        count: Arc::new(Default::default()),
    };
    for _ in 0..num_threads {
        let opt0 = opt.clone();
        let url = format!("http://{}:{}/", rand_localhost(), opt0.port);
        info!("url: {:?}", url);
        let url = url.parse::<hyper::Uri>().expect("invalid uri");
        let connections = opt.connections / num_threads;

        let counter = counter.clone();

        let client = hyper::Client::builder().build(
            hyper::client::HttpConnector::new_with_executor(NullExecutor, None),
        );
        tokio::spawn(
            run_client(client, url, connections, counter)
                .map_err(|e| eprintln!("error on client: {:?}", e)),
        );
    }

    let tick_interval = std::time::Duration::new(1, 0);
    let counter_tick = counter.clone();
    let f_tick = tokio_timer::Interval::new(Instant::now(), tick_interval)
        .map_err(|_e| panic!("Failed to set tick"))
        .fold((counter_tick, 0), |(counter, prev), _| {
            let count: usize = counter.count.load(atomic::Ordering::SeqCst);
            info!("count: {:?}, {} tps", counter.count, count - prev);
            Ok((counter, count))
        })
        .map(|_| ())
        .into_future();

    let f_client = tokio_timer::Delay::new(Instant::now() + Duration::from_secs(1))
        .then(move |_| f_tick)
        .map_err(|_e| error!("error on client"));
    let f_server = run_server(opt).map_err(|_e| error!("error on server"));

    let f = tokio_timer::Timeout::new(f_client.select(f_server), duration_timeout)
        .map(|_| ())
        .map_err(|_e| error!("benchmark finished"));

    tokio::run(f);
    Ok(())
}

#[derive(StructOpt, Debug, Clone)]
#[structopt(name = "server")]
struct Opt {
    #[structopt(long = "port", default_value = "8010")]
    port: u16,
    #[structopt(short = "n", default_value = "10")]
    connections: usize,
    #[structopt(short = "t", default_value = "4")]
    threads: usize,
    #[structopt(short = "d", default_value = "10")]
    duration: i64,
}

fn main() {
    env_logger::init();

    use structopt::StructOpt;

    let opt = Opt::from_args();
    run(&opt).expect("failed to run");
}
