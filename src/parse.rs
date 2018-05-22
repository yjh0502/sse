use std;

use client::Event;

#[derive(PartialEq, Eq, Debug)]
pub enum ParseError {
    InvalidUtf8,
    UnknownField,
    InvalidLine,
    Invalid,
    Other,
}

impl From<std::str::Utf8Error> for ParseError {
    fn from(_e: std::str::Utf8Error) -> Self {
        ParseError::InvalidUtf8
    }
}

// client
fn parse_sse_chunk(s: &str) -> Result<Event, ParseError> {
    let mut event = Event::default();
    for line in s.split('\n') {
        if line.is_empty() {
            continue;
        }

        let mut tup = line.splitn(2, ": ");
        let first = tup.next().ok_or(ParseError::InvalidLine)?;
        let second = tup.next().ok_or(ParseError::InvalidLine)?;

        if first.is_empty() {
            return Err(ParseError::Invalid);
        }

        match first {
            "event" => {
                if !event.event.is_empty() {
                    // should be only one `event` row
                    return Err(ParseError::Invalid);
                }
                event.event = second.to_owned();
            }
            "id" => {
                if event.id.is_some() {
                    // should be only one `event` row
                    return Err(ParseError::Invalid);
                }
                event.id = Some(second.to_owned());
            }
            "data" => {
                event.data += second;
                event.data += "\n";
            }
            _ => {
                return Err(ParseError::UnknownField);
            }
        }
    }

    if event.event.is_empty() {
        return Err(ParseError::Invalid);
    }

    // remove last LINE FEED
    event.data.pop();

    Ok(event)
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

pub fn parse_sse_chunks(mut s: &[u8]) -> Result<(Vec<Event>, Vec<u8>), ParseError> {
    let mut out = Vec::new();

    while let Some(pos) = find_subsequence(s, b"\n\n") {
        let msg_bytes = &s[..pos];
        s = &s[(pos + 2)..];

        let msg_chunk = std::str::from_utf8(msg_bytes)?;
        out.push(parse_sse_chunk(msg_chunk)?);
    }

    Ok((out, s.to_owned()))
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
            id: None,
            event: "foo".to_owned(),
            data: "{\n\"foo\":\"bar\"\n}".to_owned(),
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

        let (ev, remain) = parse_sse_chunks(s.as_bytes()).expect("should not fail on parse");
        assert_eq!(expected_ev, ev);
        let expected: &[u8] = b"event: aa" as &[u8];
        assert_eq!(expected, remain.as_slice());
    }
}
