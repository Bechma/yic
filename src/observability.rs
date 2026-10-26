use axum::{
    body::{Body, Bytes},
    extract::{MatchedPath, Request},
    http::HeaderValue,
    middleware::Next,
    response::Response,
};
use http_body::{Body as HttpBody, Frame, SizeHint};
use std::{
    error::Error,
    pin::Pin,
    task::{Context, Poll},
    time::Instant,
};
use tracing::{Dispatch, Instrument, Span};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

/// Install stdout tracing and bridge records from the `log` crate into it.
/// Applications embedding this router can install their own subscriber instead.
pub fn init_tracing() -> Result<(), Box<dyn Error + Send + Sync>> {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "warn,yic=info".into());
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(filter)?)
        .with_writer(std::io::stdout)
        .with_ansi(false)
        .try_init()
}

pub(crate) async fn trace_request(request: Request, next: Next) -> Response {
    // Generate a random UUID for every request, including across restarts and
    // server instances. Never record raw
    // URIs, query strings, headers, bodies, or client-provided correlation IDs.
    let request_id = Uuid::new_v4();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("unmatched", MatchedPath::as_str);
    let span = tracing::info_span!(
        "http.request",
        request_id = %request_id,
        method = %request.method(),
        route,
        status = tracing::field::Empty,
    );
    let mut lifecycle = Lifecycle {
        span: span.clone(),
        dispatch: tracing::dispatcher::get_default(Clone::clone),
        started: Instant::now(),
        bytes: 0,
        expected_bytes: None,
        finished: false,
    };
    async move {
        tracing::info!("Request started");
        let mut response = next.run(request).await;
        lifecycle.span.record("status", response.status().as_u16());
        tracing::info!(
            latency_ms = lifecycle.started.elapsed().as_millis() as u64,
            "Response headers ready"
        );
        response.headers_mut().insert(
            "x-request-id",
            HeaderValue::from_str(&request_id.to_string()).expect("UUID is a valid header value"),
        );
        let (parts, inner) = response.into_parts();
        lifecycle.expected_bytes = parts
            .headers
            .get(axum::http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok()?.parse().ok())
            .or_else(|| inner.size_hint().exact());
        if inner.is_end_stream() {
            lifecycle.finish("complete");
        }
        Response::from_parts(parts, Body::new(TracedBody { inner, lifecycle }))
    }
    .instrument(span)
    .await
}

struct Lifecycle {
    span: Span,
    dispatch: Dispatch,
    started: Instant,
    bytes: u64,
    expected_bytes: Option<u64>,
    finished: bool,
}

impl Lifecycle {
    fn finish(&mut self, outcome: &'static str) {
        if self.finished {
            return;
        }
        self.finished = true;
        tracing::dispatcher::with_default(&self.dispatch, || {
            let _entered = self.span.enter();
            let latency_ms = self.started.elapsed().as_millis() as u64;
            if outcome == "complete" {
                tracing::info!(
                    outcome,
                    latency_ms,
                    bytes_sent = self.bytes,
                    "Request completed"
                );
            } else {
                // Body errors can contain signed URLs; record only the outcome.
                tracing::warn!(
                    outcome,
                    latency_ms,
                    bytes_sent = self.bytes,
                    "Request interrupted"
                );
            }
        });
    }
}

impl Drop for Lifecycle {
    fn drop(&mut self) {
        // Hyper can stop polling at Content-Length without asking a streaming
        // body for a final None. Count a fully delivered known length as done.
        let outcome = if self.expected_bytes == Some(self.bytes) {
            "complete"
        } else {
            "cancelled"
        };
        self.finish(outcome);
    }
}

// Preserve frames (including trailers), size hints and end-of-stream semantics.
// A handler returning headers is not completion of a streaming download. Each
// poll re-enters its original request context, even on a different executor task.
struct TracedBody {
    inner: Body,
    lifecycle: Lifecycle,
}

impl HttpBody for TracedBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
        let this = self.get_mut();
        let dispatch = this.lifecycle.dispatch.clone();
        let span = this.lifecycle.span.clone();
        tracing::dispatcher::with_default(&dispatch, || {
            let _entered = span.enter();
            let frame = Pin::new(&mut this.inner).poll_frame(cx);
            match &frame {
                Poll::Ready(Some(Ok(frame))) => {
                    if let Some(data) = frame.data_ref() {
                        this.lifecycle.bytes += data.len() as u64;
                    }
                    if this.inner.is_end_stream() {
                        this.lifecycle.finish("complete");
                    }
                }
                Poll::Ready(None) => this.lifecycle.finish("complete"),
                Poll::Ready(Some(Err(_))) => this.lifecycle.finish("body_error"),
                Poll::Pending => {}
            }
            frame
        })
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests;
