//! Incremental Server-Sent Events parser. Feed arbitrary byte chunks (which
//! may split lines, CRLF pairs, or UTF-8 sequences anywhere) and receive
//! complete events on blank-line dispatch.

/// One dispatched SSE event.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SseEvent {
    /// Value of the last `event:` field before dispatch, if any.
    pub event: Option<String>,
    /// All `data:` lines joined with `\n`.
    pub data: String,
    /// Last-seen `id:` value (persists across events, per the SSE spec).
    pub id: Option<String>,
}

#[derive(Debug, Default)]
pub struct SseParser {
    buf: Vec<u8>,
    event: Option<String>,
    data: Vec<String>,
    last_id: Option<String>,
}

/// Returns `(line_end, next_line_start)` for the first complete line, handling
/// `\n`, `\r\n`, and lone `\r`. A trailing `\r` is deferred: it may be the
/// first half of a CRLF split across chunks.
fn find_line(buf: &[u8]) -> Option<(usize, usize)> {
    for (i, b) in buf.iter().enumerate() {
        match b {
            b'\n' => return Some((i, i + 1)),
            b'\r' => {
                if i + 1 < buf.len() {
                    let skip = if buf[i + 1] == b'\n' { 2 } else { 1 };
                    return Some((i, i + skip));
                }
                return None;
            }
            _ => {}
        }
    }
    None
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feeds a chunk and returns any events completed by it.
    pub fn push(&mut self, chunk: &[u8]) -> Vec<SseEvent> {
        self.buf.extend_from_slice(chunk);
        let mut out = Vec::new();
        while let Some((end, next)) = find_line(&self.buf) {
            let line: Vec<u8> = self.buf[..end].to_vec();
            self.buf.drain(..next);
            if let Some(ev) = self.process_line(&line) {
                out.push(ev);
            }
        }
        out
    }

    /// Call at end-of-stream. Lenient extension to the SSE spec: a final
    /// event whose terminating blank line never arrived is still dispatched,
    /// since some servers close the connection right after the last frame.
    pub fn finish(&mut self) -> Option<SseEvent> {
        if !self.buf.is_empty() {
            let mut line = std::mem::take(&mut self.buf);
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            if let Some(ev) = self.process_line(&line) {
                return Some(ev);
            }
        }
        self.dispatch()
    }

    fn process_line(&mut self, line: &[u8]) -> Option<SseEvent> {
        if line.is_empty() {
            return self.dispatch();
        }
        let line = String::from_utf8_lossy(line);
        if line.starts_with(':') {
            return None; // comment
        }
        let (field, value) = match line.find(':') {
            Some(idx) => {
                let v = &line[idx + 1..];
                (&line[..idx], v.strip_prefix(' ').unwrap_or(v))
            }
            None => (line.as_ref(), ""),
        };
        match field {
            "data" => self.data.push(value.to_owned()),
            "event" => self.event = Some(value.to_owned()),
            "id" if !value.contains('\0') => self.last_id = Some(value.to_owned()),
            _ => {} // "retry" and unknown fields ignored
        }
        None
    }

    fn dispatch(&mut self) -> Option<SseEvent> {
        let event = self.event.take();
        if self.data.is_empty() {
            return None;
        }
        let data = std::mem::take(&mut self.data).join("\n");
        Some(SseEvent {
            event,
            data,
            id: self.last_id.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect_all(parser: &mut SseParser, input: &[u8], chunk_size: usize) -> Vec<SseEvent> {
        let mut out = Vec::new();
        for chunk in input.chunks(chunk_size.max(1)) {
            out.extend(parser.push(chunk));
        }
        out.extend(parser.finish());
        out
    }

    #[test]
    fn simple_event() {
        let mut p = SseParser::new();
        let evs = p.push(b"data: hello\n\n");
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data, "hello");
        assert_eq!(evs[0].event, None);
    }

    #[test]
    fn every_chunk_boundary_yields_same_events() {
        let input: &[u8] = b"data: {\"a\":1}\r\n\r\nevent: ping\r\ndata: two\r\ndata: lines\r\n\r\n";
        let mut reference = SseParser::new();
        let expected = collect_all(&mut reference, input, input.len());
        assert_eq!(expected.len(), 2);
        assert_eq!(expected[0].data, "{\"a\":1}");
        assert_eq!(expected[1].data, "two\nlines");
        assert_eq!(expected[1].event.as_deref(), Some("ping"));
        for size in 1..input.len() {
            let mut p = SseParser::new();
            let got = collect_all(&mut p, input, size);
            assert_eq!(got, expected, "chunk size {size} diverged");
        }
    }

    #[test]
    fn crlf_split_across_chunks() {
        let mut p = SseParser::new();
        assert!(p.push(b"data: a\r").is_empty()); // trailing CR deferred
        let evs = p.push(b"\n\r\n");
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data, "a");
    }

    #[test]
    fn lone_cr_line_endings() {
        let mut p = SseParser::new();
        let evs = p.push(b"data: x\rdata: y\r\r ");
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data, "x\ny");
    }

    #[test]
    fn comments_and_unknown_fields_ignored() {
        let mut p = SseParser::new();
        let evs = p.push(b": keepalive\nretry: 1000\nfoo: bar\ndata: v\n\n");
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data, "v");
    }

    #[test]
    fn id_field_captured_and_persists() {
        let mut p = SseParser::new();
        let evs = p.push(b"id: 7\ndata: a\n\ndata: b\n\n");
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].id.as_deref(), Some("7"));
        assert_eq!(evs[1].id.as_deref(), Some("7"));
    }

    #[test]
    fn empty_data_not_dispatched() {
        let mut p = SseParser::new();
        assert!(p.push(b"event: noop\n\n").is_empty());
        assert!(p.finish().is_none());
    }

    #[test]
    fn no_space_after_colon() {
        let mut p = SseParser::new();
        let evs = p.push(b"data:tight\n\n");
        assert_eq!(evs[0].data, "tight");
    }

    #[test]
    fn finish_flushes_unterminated_event() {
        let mut p = SseParser::new();
        assert!(p.push(b"data: tail").is_empty());
        let ev = p.finish();
        assert_eq!(ev.map(|e| e.data), Some("tail".to_owned()));
    }

    #[test]
    fn multibyte_utf8_split_across_chunks() {
        let input = "data: héllo✓\n\n".as_bytes();
        for size in 1..input.len() {
            let mut p = SseParser::new();
            let evs = collect_all(&mut p, input, size);
            assert_eq!(evs.len(), 1, "chunk size {size}");
            assert_eq!(evs[0].data, "héllo✓", "chunk size {size}");
        }
    }
}
