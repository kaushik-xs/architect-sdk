//! W3C Trace Context (`traceparent`) middleware.
//!
//! On each request it reads the incoming `traceparent` header, extracts the
//! `trace-id`, and opens a `tracing` span carrying it — so every log event
//! emitted while handling the request automatically includes `trace_id` with no
//! change at the log call site. When the header is absent or malformed a fresh
//! **root** trace id is generated, so every request is traceable end-to-end.
//!
//! The resolved trace id is also placed in a task-local for the duration of the
//! request, so the SDK's own outbound clients (events, authrs) can continue the
//! trace via [`outbound_traceparent`]. Finally it is echoed back on the response
//! as a `traceparent` value so callers can correlate.
//!
//! Header format (W3C): `00-<32-hex trace-id>-<16-hex span-id>-<2-hex flags>`.

use axum::{
    extract::Request,
    http::{header::HeaderValue, HeaderName},
    middleware::{from_fn, FromFnLayer, Next},
    response::Response,
};
use std::future::Future;
use std::pin::Pin;
use tracing::Instrument;
use uuid::Uuid;

/// Header name carrying the W3C trace context.
pub const TRACEPARENT_HEADER: &str = "traceparent";

tokio::task_local! {
    /// The current request's 32-hex trace id, set by the ingress middleware for
    /// the duration of the request. Read via [`current_trace_id`].
    static CURRENT_TRACE_ID: String;
}

/// The trace id resolved for the current request, stored as a request extension
/// so handlers can read it explicitly. Always a 32-hex trace id (never empty).
#[derive(Clone, Debug)]
pub struct TraceId(pub String);

type BoxFut = Pin<Box<dyn Future<Output = Response> + Send>>;
type TraceFn = fn(Request, Next) -> BoxFut;

/// Middleware layer that resolves a `traceparent` trace id and attaches it to
/// every log line for the request.
///
/// Apply once on the top-level router:
/// ```ignore
/// let app = router.layer(architect_sdk::middleware::trace_id_layer());
/// ```
pub fn trace_id_layer() -> FromFnLayer<TraceFn, (), (Request,)> {
    from_fn(trace_id_mw as TraceFn)
}

/// The current request's trace id (32 lowercase hex), when a request handled
/// through [`trace_id_layer`] is on the current task. Returns `None` outside a
/// request, or on a task `spawn`ed away from the request task (task-locals do
/// not propagate across `tokio::spawn`).
pub fn current_trace_id() -> Option<String> {
    CURRENT_TRACE_ID.try_with(|t| t.clone()).ok()
}

/// A `traceparent` header value that continues the current request's trace on an
/// outbound call, with a fresh span id for this hop. `None` when no trace is in
/// scope (see [`current_trace_id`]).
pub fn outbound_traceparent() -> Option<String> {
    current_trace_id().map(|tid| format_traceparent(&tid))
}

/// Format `00-<trace-id>-<span-id>-01` with a fresh random span id. `flags=01`
/// (sampled) marks the trace as recorded.
fn format_traceparent(trace_id: &str) -> String {
    let span_id = &Uuid::new_v4().simple().to_string()[..16];
    format!("00-{trace_id}-{span_id}-01")
}

/// Extract and validate the `trace-id` from a W3C `traceparent` value. Returns
/// `None` when the shape is wrong, the version is `ff` (invalid), or the trace id
/// is all zeros (also invalid) — the caller then starts a fresh root trace.
fn parse_trace_id(traceparent: &str) -> Option<String> {
    let parts: [&str; 4] = {
        let mut it = traceparent.trim().split('-');
        let a = it.next()?;
        let b = it.next()?;
        let c = it.next()?;
        let d = it.next()?;
        if it.next().is_some() {
            return None; // more than 4 segments
        }
        [a, b, c, d]
    };
    let [version, trace_id, parent_id, flags] = parts;
    let is_hex = |s: &str, n: usize| s.len() == n && s.bytes().all(|b| b.is_ascii_hexdigit());
    if !is_hex(version, 2) || !is_hex(trace_id, 32) || !is_hex(parent_id, 16) || !is_hex(flags, 2) {
        return None;
    }
    if version.eq_ignore_ascii_case("ff") {
        return None;
    }
    let tid = trace_id.to_ascii_lowercase();
    if tid.bytes().all(|b| b == b'0') {
        return None;
    }
    Some(tid)
}

/// Generate a new root trace id: a random UUID as 32 lowercase hex chars.
fn new_trace_id() -> String {
    Uuid::new_v4().simple().to_string()
}

fn trace_id_mw(mut req: Request, next: Next) -> BoxFut {
    // Continue an incoming W3C trace, or start a fresh root trace when the header
    // is missing or malformed.
    let trace_id = req
        .headers()
        .get(TRACEPARENT_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_trace_id)
        .unwrap_or_else(new_trace_id);

    let method = req.method().clone();
    let path = req.uri().path().to_string();

    // Make the id available to handlers that want it explicitly.
    req.extensions_mut().insert(TraceId(trace_id.clone()));

    let span = tracing::info_span!(
        "request",
        trace_id = %trace_id,
        method = %method,
        path = %path,
    );
    // Echo a traceparent (this server's span) back so clients can correlate.
    let response_tp = format_traceparent(&trace_id);

    Box::pin(
        CURRENT_TRACE_ID
            .scope(trace_id, async move {
                let mut response = next.run(req).await;
                if let Ok(value) = HeaderValue::from_str(&response_tp) {
                    response
                        .headers_mut()
                        .insert(HeaderName::from_static(TRACEPARENT_HEADER), value);
                }
                response
            })
            .instrument(span),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request as HttpRequest, routing::get, Router};
    use std::sync::{Arc, Mutex};
    use tower::ServiceExt;

    #[test]
    fn parse_trace_id_accepts_valid_traceparent() {
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        assert_eq!(
            parse_trace_id(tp).as_deref(),
            Some("4bf92f3577b34da6a3ce929d0e0e4736")
        );
    }

    #[test]
    fn parse_trace_id_rejects_malformed_and_zero() {
        assert_eq!(parse_trace_id("garbage"), None);
        assert_eq!(parse_trace_id("00-abc-00f067aa0ba902b7-01"), None); // short trace-id
        assert_eq!(
            parse_trace_id("ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            None
        ); // invalid version
        assert_eq!(
            parse_trace_id("00-00000000000000000000000000000000-00f067aa0ba902b7-01"),
            None
        ); // all-zero trace-id
    }

    /// A `MakeWriter` that appends all output to a shared buffer, so a test can
    /// inspect the JSON log lines the subscriber emitted.
    #[derive(Clone)]
    struct BufWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for BufWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufWriter {
        type Writer = BufWriter;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    async fn handler() -> &'static str {
        // Emits a log with no explicit trace_id; the request span must supply it.
        tracing::info!("handling request");
        "ok"
    }

    fn app() -> Router {
        Router::new().route("/x", get(handler)).layer(trace_id_layer())
    }

    /// Parse the trace-id out of a response `traceparent` header value.
    fn resp_trace_id(header: Option<&str>) -> Option<String> {
        header.and_then(parse_trace_id)
    }

    #[test]
    fn incoming_traceparent_flows_into_logs_and_response() {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(true)
            .with_max_level(tracing::Level::TRACE)
            .with_writer(BufWriter(buf.clone()))
            .finish();

        // Pin this as the process-wide default so the `tracing` macro level gate
        // stays open regardless of other tests' thread-local subscribers (a
        // thread with no subscriber otherwise drags the global max level to OFF,
        // short-circuiting our `info!` before it reaches the buffer). This test
        // owns the only global-default call in the crate, so it always succeeds.
        tracing::subscriber::set_global_default(subscriber)
            .expect("no other test should set a global subscriber");

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let incoming = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        let echoed = rt.block_on(async {
            let req = HttpRequest::builder()
                .uri("/x")
                .header(TRACEPARENT_HEADER, incoming)
                .body(Body::empty())
                .unwrap();
            let resp = app().oneshot(req).await.unwrap();
            resp.headers()
                .get(TRACEPARENT_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(String::from)
        });

        // Echoed back as a valid traceparent carrying the same trace-id.
        assert_eq!(
            resp_trace_id(echoed.as_deref()).as_deref(),
            Some("4bf92f3577b34da6a3ce929d0e0e4736")
        );

        // Present as a top-level field in the JSON log emitted inside the handler.
        let logs = String::from_utf8(buf.lock().unwrap().clone()).unwrap();
        assert!(
            logs.contains("\"trace_id\":\"4bf92f3577b34da6a3ce929d0e0e4736\""),
            "expected trace_id in JSON logs, got: {logs}"
        );
        assert!(logs.contains("handling request"));
    }

    #[test]
    fn missing_header_generates_root_trace() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let echoed = rt.block_on(async {
            let req = HttpRequest::builder()
                .uri("/x")
                .body(Body::empty())
                .unwrap();
            let resp = app().oneshot(req).await.unwrap();
            resp.headers()
                .get(TRACEPARENT_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(String::from)
        });
        // A fresh root trace id is generated and echoed as a valid traceparent.
        let tid = resp_trace_id(echoed.as_deref());
        assert!(
            tid.as_deref().map(|t| t.len() == 32).unwrap_or(false),
            "expected a generated 32-hex trace id, got: {echoed:?}"
        );
    }
}
