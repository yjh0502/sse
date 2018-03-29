use Event;

#[derive(PartialEq, Eq, Debug)]
pub enum ParseError {
    InvalidLine,
    Invalid,
    Other,
}

// client
fn parse_sse_chunk(s: &str) -> Result<Event, ParseError> {
    let mut event = Event::default();
    for line in s.split("\n") {
        if line.is_empty() {
            continue;
        }

        let mut tup = line.splitn(2, ": ");
        let first = tup.next().ok_or(ParseError::InvalidLine)?;
        let second = tup.next().ok_or(ParseError::InvalidLine)?;

        if first.is_empty() {
            return Err(ParseError::Invalid);
        }

        if first == "event" {
            if !event.event.is_empty() {
                // should be only one `event` row
                return Err(ParseError::Invalid);
            }
            event.event = second.to_owned();
        } else {
            event.data += second;
        }
    }
    if event.event.is_empty() {
        return Err(ParseError::Invalid);
    }

    Ok(event)
}

pub fn parse_sse_chunks(s: &str) -> Result<(Vec<Event>, String), ParseError> {
    let mut out = Vec::new();

    let mut msgs = s.split("\n\n");
    let mut msg_chunk = msgs.next().ok_or(ParseError::Other)?;
    for next_msg_chunk in msgs {
        out.push(parse_sse_chunk(msg_chunk)?);
        msg_chunk = next_msg_chunk;
    }

    Ok((out, msg_chunk.to_owned()))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_chunk() {
        let s = r#"event: foo
data: test
"#;

        let ev = Event {
            event: "foo".to_owned(),
            data: "test".to_owned(),
        };

        assert_eq!(Ok(ev), parse_sse_chunk(s));
    }

    #[test]
    fn test_chunk_multiline() {
        let s = r#"event: foo
data: {
data: "foo":"bar"
data: }
"#;

        let ev = Event {
            event: "foo".to_owned(),
            data: r#"{"foo":"bar"}"#.to_owned(),
        };

        assert_eq!(Ok(ev), parse_sse_chunk(s));
    }

    #[test]
    fn test_two_event() {
        let s = r#"event: foo
data: test
event: bar
"#;
        assert!(parse_sse_chunk(s).is_err());
    }

    #[test]
    fn test_empty_data() {
        let s = r#"event: foo
data:
"#;
        assert!(parse_sse_chunk(s).is_err());
    }

    #[test]
    fn test_chunks() {
        let s = r#"event: foo
data: test

event: bar
data: test2

event: aa"#;

        let expected_ev = vec![
            Event {
                event: "foo".to_owned(),
                data: "test".to_owned(),
            },
            Event {
                event: "bar".to_owned(),
                data: "test2".to_owned(),
            },
        ];

        let (ev, remain) = parse_sse_chunks(s).expect("should not fail on parse");
        assert_eq!(expected_ev, ev);
        assert_eq!("event: aa", remain);
    }
}
