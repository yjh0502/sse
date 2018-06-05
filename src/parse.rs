use std;

use client::Event;

#[derive(PartialEq, Eq, Debug)]
pub enum ParseError {
    InvalidUtf8,
    InvalidLine,
}

impl From<std::str::Utf8Error> for ParseError {
    fn from(_e: std::str::Utf8Error) -> Self {
        ParseError::InvalidUtf8
    }
}

fn parse_sse_chunk(s: &str) -> Result<Option<Event>, ParseError> {
    let mut event = Event::default();

    for mut line in s.split('\n') {
        {
            let len = line.len();
            if len > 0 && line.as_bytes()[len - 1] == b'\r' {
                // remote '\r'
                line = &line[..len - 1];
            }
        }

        let mut tup = line.splitn(2, ':');
        // should not be reached because chunk should not contain an empty line
        let key = tup.next().ok_or(ParseError::InvalidLine)?;
        // if there's no colon, a string will be a key without value
        let mut val = tup.next().unwrap_or("");
        if val.len() > 0 && val.as_bytes()[0] == b' ' {
            // skip space character after colon
            val = &val[1..];
        }

        match key {
            "" => {
                // starts with colon, skipping
                continue;
            }
            "event" => {
                // use latest 'event' value
                event.event = val.to_owned();
            }
            "id" => {
                // use latest 'id' value
                event.id = Some(val.to_owned());
            }
            "data" => {
                event.data += val;
                event.data += "\n";
            }
            _ => {
                // ignore unknown fields
                continue;
            }
        }
    }

    // remove last LINE FEED
    event.data.pop();

    if event.data.is_empty() {
        return Ok(None);
    }

    Ok(Some(event))
}

pub fn parse_sse_chunks(s: Vec<u8>) -> Result<(Vec<Event>, Vec<u8>), ParseError> {
    let mut out = Vec::new();

    let mut start_idx = 0;
    let mut end_idx = 0;
    for mut line in s.split(|b| *b == b'\n') {
        end_idx += line.len() + 1;

        let len = line.len();
        if len > 0 && line[len - 1] == b'\r' {
            line = &line[..len - 1];
        }

        if !line.is_empty() {
            continue;
        }
        // last chunk without newline
        if end_idx > s.len() {
            break;
        }

        let chunk_bytes = &s[start_idx..end_idx];
        start_idx = end_idx;

        let chunk_str = std::str::from_utf8(chunk_bytes)?;
        if let Some(chunk) = parse_sse_chunk(chunk_str)? {
            out.push(chunk);
        }
    }

    if start_idx == 0 {
        // skip allocation if it fails to parse messages from a buffer
        return Ok((out, s));
    }

    Ok((out, s[start_idx..].to_owned()))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_chunk() {
        let s = r#"event: foo
data: test
id: 100
"#;

        let ev = Event {
            id: Some("100".to_owned()),
            event: "foo".to_owned(),
            data: "test".to_owned(),
        };

        assert_eq!(Ok(Some(ev)), parse_sse_chunk(s));
    }

    #[test]
    fn test_chunk_multiline() {
        let s = r#"event: foo
data: {
data: "foo":"bar"
data: }
"#;

        let ev = Event {
            id: None,
            event: "foo".to_owned(),
            data: "{\n\"foo\":\"bar\"\n}".to_owned(),
        };

        assert_eq!(Ok(Some(ev)), parse_sse_chunk(s));
    }

    #[test]
    fn test_two_event() {
        let s = r#"event: foo
data: test
event: bar
"#;
        let ev = parse_sse_chunk(s)
            .expect("should not fail on parse")
            .expect("should not be None");
        assert_eq!(ev.event, "bar");
        assert_eq!(ev.data, "test");
    }

    #[test]
    fn test_empty_data() {
        let s = r#"event: foo
data:
"#;
        assert_eq!(Ok(None), parse_sse_chunk(s));
    }

    #[test]
    fn test_data_no_event() {
        let s = r#"data:foo
"#;
        let parsed = parse_sse_chunk(s)
            .expect("should not fail on parse")
            .expect("should not be None");
        assert_eq!(parsed.event, "");
        assert_eq!(parsed.data, "foo");
    }

    #[test]
    fn test_data_emptyline() {
        let s = r#"data
data
"#;
        let parsed = parse_sse_chunk(s)
            .expect("should not fail on parse")
            .expect("should not be None");
        assert_eq!(parsed.event, "");
        assert_eq!(parsed.data, "\n");
    }

    #[test]
    fn test_ping() {
        let s = ":ping";
        let parsed = parse_sse_chunk(s).expect("should not fail on parse");
        assert!(parsed.is_none());
    }

    #[test]
    fn test_chunks() {
        let s = r#"event: foo
data: test

event: bar
data: test2

:ping

event: aa"#;

        let expected_ev = vec![
            Event {
                id: None,
                event: "foo".to_owned(),
                data: "test".to_owned(),
            },
            Event {
                id: None,
                event: "bar".to_owned(),
                data: "test2".to_owned(),
            },
        ];

        let (ev, remain) = parse_sse_chunks(Vec::from(s)).expect("should not fail on parse");
        assert_eq!(expected_ev, ev);
        let expected: &[u8] = b"event: aa" as &[u8];
        assert_eq!(expected, remain.as_slice());
    }
}
