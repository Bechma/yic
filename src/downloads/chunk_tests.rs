use super::*;
use axum::{Router, body::to_bytes, extract::State, routing::get};
use std::sync::Mutex;

#[derive(Clone)]
struct ChunkMock {
    bytes: Arc<Vec<u8>>,
    calls: Arc<Mutex<Vec<(u64, u64)>>>,
    mode: &'static str,
}

async fn chunk(State(mock): State<ChunkMock>, headers: HeaderMap) -> Response {
    let text = headers[header::RANGE]
        .to_str()
        .unwrap()
        .strip_prefix("bytes=")
        .unwrap();
    let (start, end) = text.split_once('-').unwrap();
    let (start, end) = (start.parse::<u64>().unwrap(), end.parse::<u64>().unwrap());
    mock.calls.lock().unwrap().push((start, end));
    if mock.mode == "forbidden" || (mock.mode == "late-forbidden" && start >= CHUNK_SIZE) {
        return Response::builder().status(403).body(Body::empty()).unwrap();
    }
    let total = mock.bytes.len();
    let end = end.min(total as u64 - 1);
    let mut bytes = mock.bytes[start as usize..=end as usize].to_vec();
    if mock.mode == "truncated" {
        bytes.pop();
    }
    let status = if mock.mode == "ignored" { 200 } else { 206 };
    let mut builder = Response::builder().status(status);
    if mock.mode != "missing-range" {
        let actual_start = if mock.mode == "wrong-range" {
            start + 1
        } else {
            start
        };
        builder = builder.header(
            header::CONTENT_RANGE,
            format!("bytes {actual_start}-{end}/{total}"),
        );
    }
    builder.body(Body::from(bytes)).unwrap()
}

struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn setup(mode: &'static str) -> (DownloadService, DownloadSource, ChunkMock, Server) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let mock = ChunkMock {
        bytes: Arc::new(
            (0..CHUNK_SIZE as usize * 3 + 117)
                .map(|i| ((i / 97 + i) % 251) as u8)
                .collect(),
        ),
        calls: Arc::default(),
        mode,
    };
    let router = Router::new()
        .route("/media", get(chunk))
        .with_state(mock.clone());
    let server = Server(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    }));
    let mut service = DownloadService::new(StreamExtractor::for_test(origin.clone()));
    service.test_origin = Some(origin.clone());
    let source = DownloadSource {
        url: origin.join("media").unwrap().to_string(),
        headers: BTreeMap::new(),
        filename: "example.m4a".into(),
        content_type: "audio/mp4".into(),
        is_subtitle: false,
        chunked: true,
        content_length: Some(mock.bytes.len() as u64),
    };
    (service, source, mock, server)
}

#[tokio::test]
async fn full_chunked_transfer_preserves_order_and_last_partial_chunk() {
    let (service, source, mock, _server) = setup("normal").await;
    let permit = service.slots.clone().acquire_owned().await.unwrap();
    let response = service
        .chunked_download(source, None, permit)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_LENGTH],
        mock.bytes.len().to_string()
    );
    assert!(!response.headers().contains_key(header::CONTENT_RANGE));
    assert_eq!(service.slots.available_permits(), 3);
    let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    assert_eq!(bytes.as_ref(), mock.bytes.as_slice());
    let mut calls = mock.calls.lock().unwrap().clone();
    calls.sort();
    assert_eq!(
        calls,
        vec![
            (0, CHUNK_SIZE - 1),
            (CHUNK_SIZE, CHUNK_SIZE * 2 - 1),
            (CHUNK_SIZE * 2, CHUNK_SIZE * 3 - 1),
            (CHUNK_SIZE * 3, CHUNK_SIZE * 3 + 116)
        ]
    );
    assert_eq!(service.slots.available_permits(), 4);
}

#[tokio::test]
async fn explicit_open_suffix_and_unsatisfiable_ranges_have_correct_client_headers() {
    for range in [
        "bytes=1000000-2100000",
        "bytes=3100000-",
        "bytes=-117",
        "bytes=9999999-",
    ] {
        let (service, source, mock, _server) = setup("normal").await;
        let header = HeaderValue::from_str(range).unwrap();
        let bounds = range_bounds(Some(&header), mock.bytes.len() as u64);
        let permit = service.slots.clone().acquire_owned().await.unwrap();
        let response = service
            .chunked_download(source, Some(header), permit)
            .await
            .unwrap();
        if let Some((start, end)) = bounds {
            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(
                response.headers()[header::CONTENT_RANGE],
                format!("bytes {start}-{end}/{}", mock.bytes.len())
            );
            let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024)
                .await
                .unwrap();
            assert_eq!(bytes.as_ref(), &mock.bytes[start as usize..=end as usize]);
        } else {
            assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
            assert_eq!(
                response.headers()[header::CONTENT_RANGE],
                format!("bytes */{}", mock.bytes.len())
            );
            assert!(mock.calls.lock().unwrap().is_empty());
        }
        assert_eq!(service.slots.available_permits(), 4);
    }
}

#[tokio::test]
async fn chunks_require_exact_ranges_and_lengths_and_abort_on_later_failures() {
    for mode in [
        "ignored",
        "missing-range",
        "wrong-range",
        "truncated",
        "forbidden",
    ] {
        let (service, source, _mock, _server) = setup(mode).await;
        let permit = service.slots.clone().acquire_owned().await.unwrap();
        assert!(
            service
                .chunked_download(source, None, permit)
                .await
                .is_err(),
            "{mode}"
        );
        assert_eq!(service.slots.available_permits(), 4);
    }
    let (service, source, _mock, _server) = setup("late-forbidden").await;
    let permit = service.slots.clone().acquire_owned().await.unwrap();
    let response = service
        .chunked_download(source, None, permit)
        .await
        .unwrap();
    assert!(
        to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await
            .is_err()
    );
    assert_eq!(service.slots.available_permits(), 4);
}
