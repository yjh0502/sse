extern crate env_logger;
extern crate futures;
extern crate hyper;
extern crate sse;
extern crate tokio_core;
extern crate tokio_timer;

use futures::future::*;
use futures::{Future, Sink, Stream};
use hyper::server::{Http, Request, Response, Service};
use hyper::{Get, StatusCode};
use sse::*;

struct Server {
    serv: EventService,
}

impl Service for Server {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        let method = req.method().clone();
        let uri = req.uri().clone();
        match (method, uri.path()) {
            (Get, "/events") => self.serv.call(req),

            (Get, "/") => {
                eprintln!("request html");
                Box::new(ok(Response::new()
                    .with_status(StatusCode::Ok)
                    .with_body(HTML)))
            }

            (method, path) => {
                eprintln!("invalid request method: {:?}, path: {:?}", method, path);
                Box::new(ok(Response::new().with_status(StatusCode::NotFound)))
            }
        }
    }
}

fn main() {
    env_logger::init();
    let addr = "0.0.0.0:8001".parse().expect("addres parsing failed");

    let mut core = tokio_core::reactor::Core::new().expect("failed to build core");
    let handle = core.handle();

    let (serv, sender) = EventService::pair2(&handle, BroadcastFlags::empty());
    let serve = Http::new()
        .serve_addr_handle(&addr, &handle, move || Ok(Server { serv: serv.clone() }))
        .expect("unable to create server");

    eprintln!("listen: {}", serve.incoming_ref().local_addr());

    let timer = tokio_timer::Timer::default();
    let timer_interval = std::time::Duration::new(1, 0);

    let start_time = std::time::Instant::now();
    let mut event_counter = 0;
    let stream_send = timer
        .interval(timer_interval)
        .map_err(|_e| {
            eprintln!("timer error");
            ()
        })
        .fold(sender, move |sender, _to| {
            let data = format!(
                "{{\"number\": \"{}\", \"time\": \"{}\"}}",
                event_counter,
                start_time.elapsed().as_secs()
            );
            event_counter += 1;
            let msg = BroadcastMessage::new("uptime", data);
            sender.send(BroadcastEvent::Message(msg)).map_err(|_e| ())
        })
        .map(|_s| {
            // drop sender
            ()
        });

    let h2 = core.handle().clone();
    let f_listen = serve
        .for_each(move |conn| {
            h2.spawn(
                conn.map(|_| ())
                    .map_err(|err| eprintln!("serve error: {:?}", err)),
            );
            Ok(())
        })
        .into_future()
        .map_err(|_e| {
            eprintln!("failed to listen: {:?}", _e);
        });

    core.run(f_listen.join(stream_send))
        .expect("unable to run server");
}

static HTML:&str = r#"<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8">
    <title>Rust Hyper Server Sent Events</title>
  </head>
  <body>
    <h1>Rust Hyper Server Sent Events</h1>
    <div id="sse-msg">
    </div>
    <script type="text/javascript">
      var evtSource = new EventSource("/events");

      evtSource.addEventListener("uptime", function(e) {
          var sseMsgDiv = document.getElementById('sse-msg');
          const obj = JSON.parse(e.data);
          sseMsgDiv.innerHTML += '<p>' + 'message number: ' + obj.number + ', time since start: ' + obj.time + '</p>';
          console.log(obj);
      }, false);
    </script>
  </body>
</html>
"#;
