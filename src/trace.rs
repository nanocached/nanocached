//! Opt-in key tracing, built only with `--features trace`.
//!
//! A key that is missing or stale after a membership change has to be followed
//! through the cache and the forward paths to tell which write or which sweep
//! decided it, and the failure that shows it (a chaos run, once in some
//! twenty) does not come back on request. With the feature on, every
//! `trace!` site whose key name starts with one of the comma-separated
//! prefixes in `NANOCACHED_TRACE_PREFIX` writes one `TRACE ...` line to
//! stderr; with the variable unset or empty nothing is written. Without the feature
//! the macro expands to nothing, so the release binaries carry no trace code
//! and evaluate none of its arguments.
//!
//! The lines do not go to stderr from the thread that traces. stderr is a
//! pipe to whatever collects the logs, and a collector that falls behind
//! (container log pressure) leaves a write blocked, which on the node's
//! request path stops every connection. A line is formatted where it is
//! traced and handed to a writer thread through a bounded queue; when the
//! queue is full the line is dropped and counted, so tracing can slow
//! nothing down. The writer puts a `TRACE dropped lines=<n>` line into the
//! stream before the next line it writes after drops, and `finish` flushes
//! at shutdown, so a gap in the lines is visible rather than silent. A node
//! killed (SIGKILL) while lines are queued loses them, with no marker: the
//! queue is not empty only when the writer is behind (the pipe is slow, or the
//! thread was scheduled late), and a line is on stderr only once the writer
//! has written it. A direct write from the tracing thread would narrow that,
//! but writing without blocking needs the descriptor in non-blocking mode,
//! which std does not offer, and a blocking write is what the queue exists to
//! avoid. `tests/e2e/incrtrace.py` takes the kills as an input instead.

/// Lines the queue holds while the writer is behind. About 100 bytes each,
/// so a stalled stderr costs the node under a megabyte.
#[cfg(feature = "trace")]
const QUEUE_LINES: usize = 8192;

/// How long `finish` waits for the writer to catch up at shutdown.
#[cfg(feature = "trace")]
const FINISH_WAIT: std::time::Duration = std::time::Duration::from_secs(2);

#[cfg(feature = "trace")]
enum Message {
    Line(String),
    /// Written out once everything before it has been, with any drops not
    /// yet reported; answered on the channel.
    Flush(std::sync::mpsc::SyncSender<()>),
}

/// A bounded queue of lines in front of a writer thread.
#[cfg(feature = "trace")]
pub(crate) struct Sink {
    queue: std::sync::mpsc::SyncSender<Message>,
    dropped: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

#[cfg(feature = "trace")]
impl Sink {
    /// Starts the writer thread over `out`, with room for `capacity` queued
    /// lines. If the thread cannot be started the queue has no reader and
    /// every line is counted as dropped, which is the same failure
    /// tracing already has when stderr is stuck.
    pub(crate) fn new(out: impl std::io::Write + Send + 'static, capacity: usize) -> Sink {
        use std::sync::atomic::AtomicU64;
        let (queue, lines) = std::sync::mpsc::sync_channel(capacity);
        let dropped = std::sync::Arc::new(AtomicU64::new(0));
        let reported = std::sync::Arc::clone(&dropped);
        let _ = std::thread::Builder::new()
            .name("trace-writer".to_string())
            .spawn(move || write_lines(out, lines, &reported));
        Sink { queue, dropped }
    }

    /// Queues `line`, or counts it as dropped when the queue is full. Never
    /// blocks.
    pub(crate) fn emit(&self, line: String) {
        if self.queue.try_send(Message::Line(line)).is_err() {
            self.dropped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Waits up to `wait` for the writer to write what is queued, then for
    /// it to report the drops not yet reported. Returns early, with
    /// the lines left queued, when the output is stuck for that long.
    pub(crate) fn finish(&self, wait: std::time::Duration) {
        let deadline = std::time::Instant::now() + wait;
        let (done, answered) = std::sync::mpsc::sync_channel(1);
        let mut message = Message::Flush(done);
        loop {
            match self.queue.try_send(message) {
                Ok(()) => break,
                Err(std::sync::mpsc::TrySendError::Full(back)) => {
                    if std::time::Instant::now() >= deadline {
                        return;
                    }
                    message = back;
                    std::thread::sleep(std::time::Duration::from_millis(5));
                }
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => return,
            }
        }
        let _ =
            answered.recv_timeout(deadline.saturating_duration_since(std::time::Instant::now()));
    }
}

#[cfg(feature = "trace")]
fn write_lines(
    mut out: impl std::io::Write,
    lines: std::sync::mpsc::Receiver<Message>,
    dropped: &std::sync::atomic::AtomicU64,
) {
    use std::sync::atomic::Ordering;
    let report = |out: &mut dyn std::io::Write| {
        let count = dropped.swap(0, Ordering::Relaxed);
        if count > 0 {
            let _ = writeln!(out, "TRACE dropped lines={count}");
        }
    };
    for message in lines {
        match message {
            Message::Line(line) => {
                report(&mut out);
                let _ = writeln!(out, "{line}");
            }
            Message::Flush(done) => {
                report(&mut out);
                let _ = out.flush();
                let _ = done.send(());
            }
        }
    }
}

#[cfg(feature = "trace")]
static SINK: std::sync::OnceLock<Sink> = std::sync::OnceLock::new();

/// Hands one formatted line to the writer thread (started on first use,
/// writing to stderr). Never blocks.
#[cfg(feature = "trace")]
pub(crate) fn emit(line: String) {
    SINK.get_or_init(|| Sink::new(std::io::stderr(), QUEUE_LINES))
        .emit(line);
}

/// Called as the process ends: gives the writer up to `FINISH_WAIT` to
/// write what is queued and report the drops, so the lines of a node that
/// is stopped normally are not lost with the process.
#[cfg(feature = "trace")]
pub(crate) fn finish() {
    if let Some(sink) = SINK.get() {
        sink.finish(FINISH_WAIT);
    }
}

#[cfg(not(feature = "trace"))]
pub(crate) fn finish() {}

#[cfg(feature = "trace")]
pub(crate) fn traced(name: &[u8]) -> bool {
    static PREFIXES: std::sync::OnceLock<Option<Vec<u8>>> = std::sync::OnceLock::new();
    match PREFIXES.get_or_init(|| {
        std::env::var("NANOCACHED_TRACE_PREFIX")
            .ok()
            .map(String::into_bytes)
    }) {
        Some(prefixes) => matches_prefix(prefixes, name),
        None => false,
    }
}

/// Whether `name` starts with one of the comma-separated `prefixes`. An
/// empty prefix (an empty variable, or a stray comma) is ignored: every
/// key starts with the empty string, so honouring it would trace them all.
#[cfg(feature = "trace")]
fn matches_prefix(prefixes: &[u8], name: &[u8]) -> bool {
    prefixes
        .split(|b| *b == b',')
        .any(|prefix| !prefix.is_empty() && name.starts_with(prefix))
}

#[cfg(feature = "trace")]
pub(crate) fn name(name: &[u8]) -> String {
    String::from_utf8_lossy(name).into_owned()
}

/// The first eight characters of a peer token, for the handoff trace lines.
/// Cut at a character boundary: a roster token is any UTF-8 a registrant
/// chose, and a byte-index slice would panic inside a multi-byte character.
#[cfg(feature = "trace")]
pub(crate) fn short_token(token: &str) -> String {
    token.chars().take(8).collect()
}

#[cfg(feature = "trace")]
macro_rules! trace {
    ($key:expr, $($arg:tt)+) => {
        if $crate::trace::traced($key) {
            $crate::trace::emit(format!("TRACE {}", format_args!($($arg)+)));
        }
    };
}

#[cfg(not(feature = "trace"))]
macro_rules! trace {
    ($key:expr, $($arg:tt)+) => {};
}

#[cfg(all(test, feature = "trace"))]
mod tests {
    use super::{Sink, matches_prefix, short_token};
    use std::io::Write;
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};

    /// An output that blocks every write until opened, like a full pipe.
    #[derive(Clone, Default)]
    struct StuckOutput {
        open: Arc<(Mutex<bool>, Condvar)>,
        written: Arc<Mutex<Vec<u8>>>,
    }

    impl StuckOutput {
        fn open(&self) {
            *self.open.0.lock().unwrap() = true;
            self.open.1.notify_all();
        }

        fn text(&self) -> String {
            String::from_utf8(self.written.lock().unwrap().clone()).unwrap()
        }
    }

    impl Write for StuckOutput {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let mut open = self.open.0.lock().unwrap();
            while !*open {
                open = self.open.1.wait(open).unwrap();
            }
            self.written.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_stuck_output_never_blocks_the_tracing_thread_and_the_drops_are_reported() {
        let output = StuckOutput::default();
        let sink = Sink::new(output.clone(), 16);

        let started = Instant::now();
        for index in 0..10_000 {
            sink.emit(format!("TRACE line {index}"));
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "10,000 lines took {:?} against an output that is stuck",
            started.elapsed()
        );

        output.open();
        sink.finish(Duration::from_secs(10));
        let text = output.text();
        let written = text
            .lines()
            .filter(|line| line.starts_with("TRACE line "))
            .count();
        let reported: usize = text
            .lines()
            .filter_map(|line| line.strip_prefix("TRACE dropped lines="))
            .map(|count| count.parse::<usize>().unwrap())
            .sum();
        assert!(
            reported > 0,
            "nothing was dropped, the queue is not bounded"
        );
        assert!(
            written <= 16 + 1,
            "{written} lines written from a 16-line queue"
        );
        assert_eq!(
            written + reported,
            10_000,
            "every line is either written or counted as dropped"
        );
    }

    #[test]
    fn finish_returns_on_time_when_the_output_stays_stuck() {
        let output = StuckOutput::default();
        let sink = Sink::new(output.clone(), 4);
        for index in 0..100 {
            sink.emit(format!("TRACE line {index}"));
        }
        let started = Instant::now();
        sink.finish(Duration::from_millis(300));
        assert!(started.elapsed() < Duration::from_secs(3));
        output.open();
    }

    #[test]
    fn lines_that_fit_the_queue_are_all_written_in_order() {
        let output = StuckOutput::default();
        output.open();
        let sink = Sink::new(output.clone(), 64);
        for index in 0..50 {
            sink.emit(format!("TRACE line {index}"));
        }
        sink.finish(Duration::from_secs(10));
        let expected: String = (0..50)
            .map(|index| format!("TRACE line {index}\n"))
            .collect();
        assert_eq!(output.text(), expected);
    }

    #[test]
    fn a_token_is_cut_at_a_character_boundary() {
        assert_eq!(short_token("0123456789abcdef"), "01234567");
        assert_eq!(short_token("abc"), "abc");
        assert_eq!(short_token(""), "");
        // Byte 8 falls inside the fifth two-byte character.
        assert_eq!(short_token("ééééééééé"), "éééééééé");
        assert_eq!(short_token("日本語日本語日本語"), "日本語日本語日本");
    }

    #[test]
    fn an_empty_variable_traces_nothing() {
        assert!(!matches_prefix(b"", b"chaos-1"));
        assert!(!matches_prefix(b"", b""));
    }

    #[test]
    fn a_stray_comma_does_not_turn_every_key_into_a_match() {
        assert!(!matches_prefix(b",", b"chaos-1"));
        assert!(!matches_prefix(b"touch-,", b"chaos-1"));
        assert!(!matches_prefix(b",touch-", b"chaos-1"));
    }

    #[test]
    fn a_key_matching_any_listed_prefix_is_traced() {
        assert!(matches_prefix(b"touch-,chaos-", b"chaos-1"));
        assert!(matches_prefix(b"touch-,chaos-", b"touch-9"));
        assert!(!matches_prefix(b"touch-,chaos-", b"other"));
    }
}
