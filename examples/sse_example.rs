extern crate env_logger;
extern crate futures;
extern crate hyper;
extern crate sse;
extern crate tokio;
extern crate tokio_timer;

use std::time::*;

use futures::future::*;
use futures::{Future, Sink, Stream};
use hyper::service::Service;
use hyper::{Body, Method, Request, Response, StatusCode};
use sse::*;

struct Server {
    serv: EventService,
}

impl Service for Server {
    type ReqBody = Body;
    type ResBody = Body;
    type Error = hyper::Error;
    type Future = Box<Future<Item = Response<Body>, Error = Self::Error> + Send>;

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let method = req.method().clone();
        let uri = req.uri().clone();
        match (method, uri.path()) {
            (Method::GET, "/events") => self.serv.call(req),

            (Method::GET, "/") => {
                eprintln!("request html");
                Box::new(ok(Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from(HTML))
                    .unwrap()))
            }

            (method, path) => {
                eprintln!("invalid request method: {:?}, path: {:?}", method, path);
                Box::new(ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap()))
            }
        }
    }
}

fn main() {
    env_logger::init();
    let addr = "0.0.0.0:8001".parse().expect("addres parsing failed");

    let (serv, sender) = EventService::pair_sse(BroadcastFlags::empty());

    let f_server = hyper::server::Server::bind(&addr)
        .serve(move || Ok::<_, hyper::Error>(Server { serv: serv.clone() }))
        .map(|_| ())
        .map_err(|e| eprintln!("failed to serve: {:?}", e));

    let timer_interval = Duration::from_secs(1);

    let start_time = std::time::Instant::now();
    let mut event_counter = 0;
    let stream_send = tokio_timer::Interval::new(Instant::now(), timer_interval)
        .map_err(|_e| {
            eprintln!("timer error");
            ()
        }).fold(sender, move |sender, _to| {
            let data = format!(
                "{{\"number\": \"{}\", \"time\": \"{}\"}}",
                event_counter,
                start_time.elapsed().as_secs()
            );
            event_counter += 1;
            let msg = BroadcastMessage::new("uptime", data);
            sender.send(BroadcastEvent::Message(msg)).map_err(|_e| ())
        }).map(|_s| {
            // drop sender
            ()
        });

    tokio::run(f_server.join(stream_send).then(|_| Ok(())));
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
