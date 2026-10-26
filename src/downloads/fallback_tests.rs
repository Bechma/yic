use super::*;
use axum::{
    Json, Router,
    body::to_bytes,
    extract::State,
    http::{Request, Uri},
    response::IntoResponse,
    routing::{get, post},
};
use serde_json::{Value, json};
use std::sync::Mutex;
use tower::ServiceExt;

const VIDEO_ID: &str = "P9iH4Wadwv4";

#[derive(Clone)]
struct Mock {
    origin: String,
    bytes: Arc<Vec<u8>>,
    calls: Arc<Mutex<Vec<String>>>,
    native_client: &'static str,
    mode: &'static str,
}

async fn player(State(mock): State<Mock>, Json(body): Json<Value>) -> Response {
    let client = body["context"]["client"]["clientName"].as_str().unwrap();
    mock.calls.lock().unwrap().push(format!("player:{client}"));
    let mweb = client == "MWEB";
    if !mweb && client != mock.native_client {
        return Json(json!({"playabilityStatus":{"status":"LOGIN_REQUIRED"}})).into_response();
    }
    if mweb && mock.mode == "extraction-failure" {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let path = if mweb { "mweb" } else { "native" };
    let track = if mock.mode == "no-track"
        || (!mweb && mock.mode == "unwanted-track")
        || (mweb && mock.mode == "missing-track")
    {
        Value::Null
    } else {
        json!({"id": if mweb && mock.mode == "wrong-track" { "en.1" } else { "es-US.4" }})
    };
    let mime = match (mweb, mock.mode) {
        (true, "wrong-type") => "video/mp4; codecs=\"avc1\"",
        (true, "wrong-codec") => "audio/mp4; codecs=\"mp4a.40.5\"",
        _ => "audio/mp4; codecs=\"mp4a.40.2\"",
    };
    let length = mock.bytes.len() + usize::from(mweb && mock.mode == "wrong-size");
    Json(json!({
        "playabilityStatus":{"status":"OK"},
        "videoDetails":{"videoId":VIDEO_ID},
        "streamingData":{"adaptiveFormats":[
            {"itag":if mweb && mock.mode == "wrong-format" {139} else {140},
             "mimeType":mime,"audioTrack":track,"contentLength":length.to_string(),
             "url":format!("{}/{path}?kind=audio",mock.origin)},
            {"itag":137,"mimeType":"video/mp4; codecs=\"avc1\"","width":1920,"height":1080,
             "contentLength":mock.bytes.len().to_string(),"url":format!("{}/{path}?kind=video",mock.origin)}
        ]},
        "captions":{"playerCaptionsTracklistRenderer":{"captionTracks":[
            {"baseUrl":format!("{}/native?kind=subtitle",mock.origin),"languageCode":"es","vssId":".es"}
        ]}}
    })).into_response()
}

async fn media(State(mock): State<Mock>, uri: Uri, headers: HeaderMap) -> Response {
    let range = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("none");
    mock.calls
        .lock()
        .unwrap()
        .push(format!("{}:{range}", uri.path()));
    if uri.path() == "/native" {
        if mock.mode == "native-416" {
            return Response::builder()
                .status(416)
                .header(
                    header::CONTENT_RANGE,
                    format!("bytes */{}", mock.bytes.len()),
                )
                .body(Body::empty())
                .unwrap();
        }
        if mock.mode == "native-success" {
            return Response::new(Body::from(mock.bytes.as_ref().clone()));
        }
        return Response::builder()
            .status(if mock.mode == "native-error" {
                500
            } else {
                403
            })
            .body(Body::from("secret signed URL and upstream diagnostics"))
            .unwrap();
    }
    let total = mock.bytes.len() as u64;
    let (start, end) = range_bounds(headers.get(header::RANGE), total).unwrap();
    if mock.mode == "mweb-forbidden" || (mock.mode == "late-forbidden" && start >= CHUNK_SIZE) {
        return Response::builder().status(403).body(Body::empty()).unwrap();
    }
    Response::builder()
        .status(206)
        .header(
            header::CONTENT_RANGE,
            format!("bytes {start}-{end}/{total}"),
        )
        .body(Body::from(
            mock.bytes[start as usize..=end as usize].to_vec(),
        ))
        .unwrap()
}

struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn setup(mode: &'static str, native_client: &'static str) -> (DownloadService, Mock, Server) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let mock = Mock {
        origin: origin.clone(),
        bytes: Arc::new(
            (0..CHUNK_SIZE as usize * 2 + 317)
                .map(|i| (i % 251) as u8)
                .collect(),
        ),
        calls: Arc::default(),
        native_client,
        mode,
    };
    let app = Router::new()
        .route("/player", post(player))
        .fallback(get(media))
        .with_state(mock.clone());
    let server = Server(tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    }));
    let mut service = DownloadService::new(StreamExtractor::for_test(
        Url::parse(&format!("{origin}/player")).unwrap(),
    ));
    service.test_origin = Some(Url::parse(&origin).unwrap());
    (service, mock, server)
}

fn audio(client: &str) -> Value {
    json!({"audio_track_id":"es-US.4","format_id":"140","source_client":client,"type":"audio"})
}

async fn request(service: DownloadService, selection: Value, range: Option<&str>) -> Response {
    let app = Router::new()
        .route("/videos/{video_id}/download", post(crate::download_video))
        .with_state(service);
    let mut request = Request::builder()
        .method("POST")
        .uri(format!("/videos/{VIDEO_ID}/download"))
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(range) = range {
        request = request.header(header::RANGE, range);
    }
    app.oneshot(request.body(Body::from(selection.to_string())).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn native_403_falls_back_and_delivers_entire_audio_or_video_with_exact_bytes() {
    for client in ["IOS", "TVHTML5"] {
        for video in [false, true] {
            let (service, mock, _server) = setup("normal", client).await;
            let selection = if video {
                json!({"type":"video","format_id":"137","source_client":client})
            } else {
                audio(client)
            };
            let response = request(service.clone(), selection, None).await;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()["x-download-source-client"], "MWEB");
            assert_eq!(
                response.headers()[header::CONTENT_LENGTH],
                mock.bytes.len().to_string()
            );
            assert!(
                response.headers()[header::CONTENT_DISPOSITION]
                    .to_str()
                    .unwrap()
                    .ends_with(if video { ".mp4\"" } else { ".m4a\"" })
            );
            assert_eq!(service.slots.available_permits(), 3);
            assert_eq!(
                to_bytes(response.into_body(), 4 * 1024 * 1024)
                    .await
                    .unwrap()
                    .as_ref(),
                mock.bytes.as_slice()
            );
            assert_eq!(service.slots.available_permits(), 4);
            let calls = mock.calls.lock().unwrap();
            assert_eq!(
                calls
                    .iter()
                    .filter(|call| call.as_str() == "player:MWEB")
                    .count(),
                1
            );
            assert_eq!(
                calls
                    .iter()
                    .filter(|call| call.starts_with("/mweb:"))
                    .count(),
                3
            );
            assert_eq!(
                calls
                    .iter()
                    .filter(|call| call.starts_with("/native:"))
                    .count(),
                1
            );
        }
    }
}

#[tokio::test]
async fn fallback_preserves_ranges_including_multi_chunk_resume_and_unsatisfiable_offsets() {
    let (service, mock, _server) = setup("normal", "IOS").await;
    for range in [
        "bytes=1000000-2100000",
        "bytes=2000000-",
        "bytes=-317",
        "bytes=9999999-",
    ] {
        let expected = range_bounds(
            Some(&HeaderValue::from_str(range).unwrap()),
            mock.bytes.len() as u64,
        );
        let response = request(service.clone(), audio("IOS"), Some(range)).await;
        if let Some((start, end)) = expected {
            assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
            assert_eq!(response.headers()["x-download-source-client"], "MWEB");
            assert_eq!(
                response.headers()[header::CONTENT_RANGE],
                format!("bytes {start}-{end}/{}", mock.bytes.len())
            );
            assert_eq!(
                to_bytes(response.into_body(), 4 * 1024 * 1024)
                    .await
                    .unwrap()
                    .as_ref(),
                &mock.bytes[start as usize..=end as usize]
            );
        } else {
            assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
            assert_eq!(
                response.headers()[header::CONTENT_RANGE],
                format!("bytes */{}", mock.bytes.len())
            );
        }
        assert_eq!(service.slots.available_permits(), 4);
    }
}

#[tokio::test]
async fn native_416_falls_back_only_for_ranges_that_fit_the_known_file() {
    let (service, mock, _server) = setup("native-416", "IOS").await;
    let response = request(service.clone(), audio("IOS"), Some("bytes=-317")).await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()["x-download-source-client"], "MWEB");
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap().as_ref(),
        &mock.bytes[mock.bytes.len() - 317..]
    );
    mock.calls.lock().unwrap().clear();
    let response = request(service.clone(), audio("IOS"), Some("bytes=9999999-")).await;
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(
        response.headers()[header::CONTENT_RANGE],
        format!("bytes */{}", mock.bytes.len())
    );
    assert!(
        mock.calls
            .lock()
            .unwrap()
            .iter()
            .all(|call| call != "player:MWEB")
    );
    assert_eq!(service.slots.available_permits(), 4);
}

#[tokio::test]
async fn fallback_never_substitutes_another_track_format_type_codec_or_file_length() {
    for mode in [
        "wrong-track",
        "wrong-format",
        "wrong-type",
        "wrong-codec",
        "wrong-size",
        "missing-track",
        "unwanted-track",
    ] {
        let (service, mock, _server) = setup(mode, "IOS").await;
        let mut selection = audio("IOS");
        if mode == "unwanted-track" {
            selection["audio_track_id"] = Value::Null;
        }
        let response = request(service.clone(), selection, None).await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY, "{mode}");
        let body: Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1024).await.unwrap()).unwrap();
        assert_eq!(
            body,
            json!({"error":"YouTube media server rejected the download","upstream_status":403}),
            "{mode}"
        );
        assert!(
            mock.calls
                .lock()
                .unwrap()
                .iter()
                .all(|call| !call.starts_with("/mweb:")),
            "{mode}"
        );
        assert_eq!(service.slots.available_permits(), 4);
    }
}

#[tokio::test]
async fn missing_audio_track_matches_only_another_missing_audio_track() {
    let (service, mock, _server) = setup("no-track", "IOS").await;
    let mut selection = audio("IOS");
    selection["audio_track_id"] = Value::Null;
    let response = request(service, selection, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap()
            .as_ref(),
        mock.bytes.as_slice()
    );
}

#[tokio::test]
async fn failed_mweb_attempt_is_bounded_and_does_not_leak_upstream_diagnostics() {
    for mode in ["mweb-forbidden", "extraction-failure"] {
        let (service, mock, _server) = setup(mode, "IOS").await;
        let response = request(service.clone(), audio("IOS"), None).await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        assert!(!text.contains("secret"));
        if mode == "mweb-forbidden" {
            assert_eq!(
                serde_json::from_slice::<Value>(&body).unwrap()["upstream_status"],
                403
            );
        }
        assert_eq!(
            mock.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|call| call.as_str() == "player:MWEB")
                .count(),
            1
        );
        assert_eq!(service.slots.available_permits(), 4);
    }
}

#[tokio::test]
async fn late_fallback_failure_aborts_the_body_and_releases_the_slot() {
    let (service, _mock, _server) = setup("late-forbidden", "IOS").await;
    let response = request(service.clone(), audio("IOS"), None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await
            .is_err()
    );
    assert_eq!(service.slots.available_permits(), 4);
}

#[tokio::test]
async fn successful_native_download_other_statuses_and_subtitles_do_not_trigger_fallback() {
    for mode in ["native-success", "native-error", "subtitle"] {
        let (service, mock, _server) = setup(mode, "IOS").await;
        let selection = if mode == "subtitle" {
            json!({"type":"subtitle","source_client":"IOS","track_id":".es","language":"es","ext":"vtt"})
        } else {
            audio("IOS")
        };
        let response = request(service.clone(), selection, None).await;
        assert_eq!(
            response.status(),
            if mode == "native-success" {
                StatusCode::OK
            } else {
                StatusCode::BAD_GATEWAY
            }
        );
        if mode == "native-success" {
            assert_eq!(response.headers()["x-download-source-client"], "IOS");
        }
        let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap();
        if mode == "native-success" {
            assert_eq!(bytes.as_ref(), mock.bytes.as_slice());
        }
        assert!(
            mock.calls
                .lock()
                .unwrap()
                .iter()
                .all(|call| call != "player:MWEB")
        );
        assert_eq!(service.slots.available_permits(), 4);
    }
}
