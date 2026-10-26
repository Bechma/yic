use super::*;
use axum::{Router, routing::get};
use futures_util::{StreamExt, stream};
use std::{
    collections::VecDeque,
    io::{self, Write},
    sync::{Arc, Mutex},
};
use tower::ServiceExt;
use tracing::instrument::WithSubscriber;

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Self;
    fn make_writer(&'a self) -> Self {
        self.clone()
    }
}

impl Capture {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
    fn subscriber(&self) -> Dispatch {
        Dispatch::new(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .without_time()
                .with_ansi(false)
                .with_writer(self.clone())
                .finish(),
        )
    }
}

async fn body() -> Body {
    Body::from_stream(stream::once(async {
        tracing::info!("Body polled");
        Ok::<_, io::Error>(Bytes::from_static(b"payload"))
    }))
}

fn router() -> Router {
    Router::new()
        .route("/items/{id}", get(body))
        .route(
            "/failure",
            get(|| async {
                Body::from_stream(
                    stream::once(async { Ok::<_, io::Error>(Bytes::from_static(b"abc")) }).chain(
                        stream::once(async {
                            Err(io::Error::other("secret signed URL must not be logged"))
                        }),
                    ),
                )
            }),
        )
        .layer(axum::middleware::from_fn(trace_request))
}

#[tokio::test]
async fn traces_body_after_handler_returns_without_logging_inputs() {
    let capture = Capture::default();
    let response = router()
        .oneshot(
            Request::builder()
                .uri("/items/secret-path?key=secret-query")
                .header("authorization", "secret-authorization")
                .header("x-request-id", "secret-caller-id")
                .body(Body::from("secret-body"))
                .unwrap(),
        )
        .with_subscriber(capture.subscriber())
        .await
        .unwrap();
    let id = response.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let parsed_id = Uuid::parse_str(&id).unwrap();
    assert_eq!(parsed_id.get_version_num(), 4);
    assert_eq!(parsed_id.get_variant(), uuid::Variant::RFC4122);
    assert_eq!(parsed_id.hyphenated().to_string(), id);
    assert!(!capture.text().contains("Request completed"));
    // Deliberately consume outside the request's scoped subscriber to verify
    // the body restores its context when the server polls it elsewhere.
    assert_eq!(
        axum::body::to_bytes(response.into_body(), 100)
            .await
            .unwrap(),
        "payload"
    );
    let logs = capture.text();
    for message in [
        "Request started",
        "Response headers ready",
        "Body polled",
        "Request completed",
    ] {
        let line = logs.lines().find(|line| line.contains(message)).unwrap();
        assert!(line.contains(&format!("request_id={id}")), "{line}");
        assert!(line.contains("route=\"/items/{id}\""), "{line}");
    }
    assert!(logs.contains("status=200"));
    assert!(logs.contains("bytes_sent=7"));
    assert!(logs.contains("latency_ms="));
    assert!(!logs.contains("secret"), "{logs}");
    assert_eq!(logs.matches("Request completed").count(), 1);
}

#[tokio::test]
async fn distinguishes_late_body_failure_from_success_and_cancellation() {
    let capture = Capture::default();
    let response = router()
        .oneshot(
            Request::builder()
                .uri("/failure")
                .body(Body::empty())
                .unwrap(),
        )
        .with_subscriber(capture.subscriber())
        .await
        .unwrap();
    assert!(
        axum::body::to_bytes(response.into_body(), 100)
            .await
            .is_err()
    );
    let logs = capture.text();
    assert!(logs.contains("outcome=\"body_error\""), "{logs}");
    assert!(logs.contains("bytes_sent=3"));
    assert!(logs.contains("status=200"));
    assert!(!logs.contains("secret"));
    assert!(!logs.contains("Request completed"));
    assert_eq!(logs.matches("Request interrupted").count(), 1);
}

#[tokio::test]
async fn dropping_response_reports_cancellation_with_request_context() {
    let capture = Capture::default();
    let response = router()
        .oneshot(
            Request::builder()
                .uri("/items/id")
                .body(Body::empty())
                .unwrap(),
        )
        .with_subscriber(capture.subscriber())
        .await
        .unwrap();
    let id = response.headers()["x-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    drop(response);
    let logs = capture.text();
    assert!(logs.contains("outcome=\"cancelled\""), "{logs}");
    let line = logs
        .lines()
        .find(|line| line.contains("Request interrupted"))
        .unwrap();
    assert!(line.contains(&format!("request_id={id}")));
    assert!(!logs.contains("Request completed"));
}

#[tokio::test]
async fn cancellation_before_headers_is_also_logged() {
    let capture = Capture::default();
    let notify = Arc::new(tokio::sync::Notify::new());
    let handler_notify = notify.clone();
    let app = Router::new()
        .route(
            "/wait",
            get(move || async move {
                handler_notify.notify_one();
                std::future::pending::<()>().await;
            }),
        )
        .layer(axum::middleware::from_fn(trace_request));
    let task = tokio::spawn(
        app.oneshot(Request::builder().uri("/wait").body(Body::empty()).unwrap())
            .with_subscriber(capture.subscriber()),
    );
    notify.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let logs = capture.text();
    assert!(logs.contains("Request started"));
    assert!(logs.contains("outcome=\"cancelled\""), "{logs}");
    assert!(!logs.contains("Response headers ready"));
}

#[tokio::test]
async fn known_length_can_complete_without_a_final_eof_poll() {
    let capture = Capture::default();
    let app = Router::new()
        .route(
            "/length",
            get(|| async { ([("content-length", "7")], body().await) }),
        )
        .layer(axum::middleware::from_fn(trace_request));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/length")
                .body(Body::empty())
                .unwrap(),
        )
        .with_subscriber(capture.subscriber())
        .await
        .unwrap();
    let mut body = response.into_body();
    let frame = futures_util::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.data_ref().unwrap(), "payload");
    // Simulate the transport dropping a body after sending Content-Length bytes.
    drop(body);
    let logs = capture.text();
    assert_eq!(logs.matches("Request completed").count(), 1, "{logs}");
    assert!(!logs.contains("Request interrupted"));
    assert!(logs.contains("bytes_sent=7"));
}

struct Frames(VecDeque<Frame<Bytes>>);
impl HttpBody for Frames {
    type Data = Bytes;
    type Error = io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, io::Error>>> {
        Poll::Ready(self.0.pop_front().map(Ok))
    }
    fn is_end_stream(&self) -> bool {
        self.0.is_empty()
    }
}

#[tokio::test]
async fn preserves_trailers_and_handles_empty_and_unmatched_responses() {
    let capture = Capture::default();
    let app = Router::new()
        .route(
            "/trailers",
            get(|| async {
                let mut trailers = axum::http::HeaderMap::new();
                trailers.insert("x-checksum", HeaderValue::from_static("abc"));
                Body::new(Frames(VecDeque::from([
                    Frame::data(Bytes::from_static(b"abc")),
                    Frame::trailers(trailers),
                ])))
            }),
        )
        .route(
            "/empty",
            get(|| async { axum::http::StatusCode::NO_CONTENT }),
        )
        .layer(axum::middleware::from_fn(trace_request));
    let mut ids = Vec::new();
    for uri in ["/trailers", "/empty", "/unknown-secret"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .with_subscriber(capture.subscriber())
            .await
            .unwrap();
        let id = Uuid::parse_str(response.headers()["x-request-id"].to_str().unwrap()).unwrap();
        assert_eq!(id.get_version_num(), 4);
        ids.push(id);
        let mut body = response.into_body();
        if uri == "/trailers" {
            let frame = futures_util::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(frame.data_ref().unwrap(), "abc");
            let frame = futures_util::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(frame.trailers_ref().unwrap()["x-checksum"], "abc");
            assert!(body.is_end_stream());
        } else {
            axum::body::to_bytes(body, 100).await.unwrap();
        }
    }
    assert_eq!(
        ids.iter().collect::<std::collections::HashSet<_>>().len(),
        3
    );
    let logs = capture.text();
    assert_eq!(logs.matches("Request completed").count(), 3, "{logs}");
    assert!(!logs.contains("Request interrupted"));
    assert!(logs.contains("status=404"));
    assert!(!logs.contains("unknown-secret"));
}
