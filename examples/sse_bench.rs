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
extern crate futures_cpupool;
extern crate rand;
extern crate sse;
extern crate tokio_core;
extern crate tokio_timer;

use std::rc::Rc;
use std::sync::{atomic, Arc};
use std::time::*;

use futures::future::*;
use futures::*;
use futures_cpupool::CpuPool;
use hyper::server::Http;
use hyper::Request;
use rand::Rng;
use tokio_core::reactor::*;

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
    req: Request,
    counter: Counter,
) -> impl Future<Item = (), Error = ()> {
    let f = client
        .request(req)
        .map_err(|e| -> () {
            panic!("failed to connect: {:?}", e);
        })
        .and_then(move |res| {
            res.body()
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
) -> impl Future<Item = (), Error = Error> {
    let clients = (0..count)
        .map(|_n| {
            let req = Request::new(hyper::Method::Get, url.clone());
            spawn_client(&client, req, counter.clone())
        })
        .collect::<Vec<_>>();

    join_all(clients)
        .map(|_| ())
        .or_else(|_e| bail!("error on client"))
}

fn run_server(opt: &Opt, handle: &Handle) -> Box<Future<Item = (), Error = Error>> {
    let addr_str = format!("0.0.0.0:{}", opt.port);
    let addr = addr_str.parse().expect("addres parsing failed");

    let (serv, sender) = EventService::pair_sse(handle, BroadcastFlags::empty());
    let serv = Rc::new(serv);

    let serve = Http::new()
        .max_buf_size(1024 * 16)
        .serve_addr_handle(&addr, handle, move || Ok(serv.clone()))
        .expect("unable to create server");

    info!("listen: {}", serve.incoming_ref().local_addr());

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

    let handle = handle.clone();
    let f_listen = serve
        .for_each(move |conn| {
            handle.spawn(
                conn.map(|_| ())
                    .map_err(|err| error!("serve error: {:?}", err)),
            );
            Ok(())
        })
        .into_future()
        .map_err(|_e| {
            error!("failed to listen: {:?}", _e);
        });

    let f = stream_send.select(f_listen).then(|_| {
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
    let mut core = Core::new().expect("failed to build core");
    let handle = core.handle();

    let duration_timeout = Duration::from_secs(opt.duration as u64);

    let num_threads = opt.threads;
    let pool = CpuPool::new(num_threads);

    let mut f_clients = Vec::with_capacity(num_threads);
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
        let f_client = pool.spawn_fn(move || {
            let mut core = Core::new().expect("failed to build core");
            let handle = core.handle();
            let client = hyper::Client::configure()
                .connector(hyper::client::HttpConnector::new_with_executor(
                    NullExecutor,
                    &handle,
                ))
                .build(&handle);

            core.run(run_client(client, url, connections, counter))
        });
        f_clients.push(f_client);
    }

    let tick_interval = std::time::Duration::new(1, 0);
    let counter_tick = counter.clone();
    let f_tick = tokio_timer::Interval::new(Instant::now(), tick_interval)
        .map_err(|_e| panic!("Failed to set tick"))
        .fold((counter_tick, 0), |(counter, prev), _| {
            let count: usize = counter.count.load(atomic::Ordering::SeqCst);
            info!("count: {:?}, {} tps", counter.count, count - prev);
            Ok::<_, Error>((counter, count))
        })
        .into_future();

    let f_client = tokio_timer::Delay::new(Instant::now() + Duration::from_secs(1))
        .then(|_| join_all(f_clients).join(f_tick).map(|_| ()));
    let f_server = run_server(opt, &handle);

    let f = tokio_timer::Deadline::new(
        f_client.select(f_server).map_err(|_e| {
            error!("error on bench");
            ()
        }),
        Instant::now() + duration_timeout,
    ).map(|_| ())
        .or_else(|e| {
            error!("benchmark finished: {:?}", e);
            Ok::<(), Error>(())
        });

    core.run(f)
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
