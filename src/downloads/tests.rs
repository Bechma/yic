use super::*;
use axum::{
    Json, Router,
    body::{Bytes, to_bytes},
    extract::State,
    http::{Request, Uri},
    routing::{get, post},
};
use serde_json::{Value, json};
use std::sync::Mutex;
use tokio::sync::Notify;
use tower::ServiceExt;

#[derive(Clone)]
struct Mock {
    origin: String,
    media_path: String,
    calls: Arc<Mutex<Vec<(String, HeaderMap)>>>,
    fail_body: Arc<Notify>,
}

async fn player(State(state): State<Mock>, Json(body): Json<Value>) -> Json<Value> {
    state
        .calls
        .lock()
        .unwrap()
        .push(("player".into(), HeaderMap::new()));
    if body["context"]["client"]["clientName"] != "IOS" {
        return Json(json!({"playabilityStatus": {"status": "LOGIN_REQUIRED"}}));
    }
    Json(json!({
        "playabilityStatus": {"status": "OK"},
        "videoDetails": {"videoId": "dQw4w9WgXcQ", "title": "Unsafe / title"},
        "streamingData": {"adaptiveFormats": [
            {"itag": 140, "mimeType": "audio/mp4; codecs=\"mp4a\"", "audioTrack": {"id": "en.1"}, "url": format!("{}{}", state.origin, state.media_path)},
            {"itag": 140, "mimeType": "audio/mp4; codecs=\"mp4a\"", "audioTrack": {"id": "es.1"}, "url": format!("{}/media?lang=es", state.origin)},
            {"itag": 137, "mimeType": "video/mp4; codecs=\"avc1\"", "url": format!("{}/media?kind=video", state.origin)}
        ]},
        "captions": {"playerCaptionsTracklistRenderer": {
            "captionTracks": [{"baseUrl": format!("{}/subtitle?sig=a%2Bb", state.origin), "languageCode": "en", "vssId": ".en", "isTranslatable": true}],
            "translationLanguages": [{"languageCode": "fr"}]
        }}
    }))
}

async fn media(State(state): State<Mock>, uri: Uri, headers: HeaderMap) -> Response {
    state
        .calls
        .lock()
        .unwrap()
        .push((uri.to_string(), headers.clone()));
    match uri.path() {
        "/redirect" | "/badredirect" => {
            let location = if uri.path() == "/redirect" {
                "/media"
            } else {
                "http://127.0.0.1:1/private"
            };
            return Response::builder()
                .status(302)
                .header(header::LOCATION, location)
                .body(Body::empty())
                .unwrap();
        }
        "/failure" => {
            return Response::builder()
                .status(403)
                .body(Body::from("private signed URL and upstream diagnostics"))
                .unwrap();
        }
        "/slow" => tokio::time::sleep(Duration::from_millis(200)).await,
        "/broken" => {
            let notify = state.fail_body;
            let chunks = futures_util::stream::unfold(0, move |step| {
                let notify = notify.clone();
                async move {
                    match step {
                        0 => Some((Ok::<_, std::io::Error>(Bytes::from_static(b"start")), 1)),
                        1 => {
                            notify.notified().await;
                            Some((Err(std::io::Error::other("upstream secret")), 2))
                        }
                        _ => None,
                    }
                }
            });
            return Response::new(Body::from_stream(chunks));
        }
        "/subtitle" => {
            return Response::new(Body::from("WEBVTT\n\n00:00.000 --> 00:01.000\nHello\n"));
        }
        _ => {}
    }
    let (status, body, range) = match headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
    {
        Some("bytes=2-5") => (206, "2345", Some("bytes 2-5/10")),
        Some("bytes=999-") => (416, "", Some("bytes */10")),
        _ => (200, "0123456789", None),
    };
    let mut response = Response::builder()
        .status(status)
        .header(header::CONTENT_LENGTH, body.len())
        .header(header::ACCEPT_RANGES, "bytes");
    if let Some(range) = range {
        response = response.header(header::CONTENT_RANGE, range);
    }
    response.body(Body::from(body)).unwrap()
}

struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn setup(media_path: &str) -> (DownloadService, Mock, Server) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let state = Mock {
        origin: origin.clone(),
        media_path: media_path.into(),
        calls: Arc::default(),
        fail_body: Arc::default(),
    };
    let router = Router::new()
        .route("/player", post(player))
        .fallback(get(media))
        .with_state(state.clone());
    let server = Server(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    }));
    let extractor = StreamExtractor::for_test(Url::parse(&format!("{origin}/player")).unwrap());
    let mut downloads = DownloadService::new(extractor);
    downloads.test_origin = Some(Url::parse(&origin).unwrap());
    (downloads, state, server)
}

fn audio() -> Value {
    json!({"type": "audio", "format_id": "140", "source_client": "IOS", "audio_track_id": "en.1"})
}

async fn request(
    service: DownloadService,
    id: &str,
    selection: Value,
    range: Option<&str>,
) -> Response {
    let router = Router::new()
        .route("/videos/{video_id}/download", post(crate::download_video))
        .with_state(service);
    let mut request = Request::builder()
        .method("POST")
        .uri(format!("/videos/{id}/download"))
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(range) = range {
        request = request.header(header::RANGE, range);
    }
    router
        .oneshot(request.body(Body::from(selection.to_string())).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn downloads_audio_video_and_translated_subtitles_from_returned_references() {
    let (service, state, _server) = setup("/media").await;
    let streams =
        serde_json::to_value(service.extractor.extract("dQw4w9WgXcQ").await.unwrap()).unwrap();
    let cases = [
        (
            streams["audio_streams"][0]["download"].clone(),
            "dQw4w9WgXcQ-140.m4a",
            "audio/mp4; codecs=\"mp4a\"",
        ),
        (
            streams["video_streams"][0]["download"].clone(),
            "dQw4w9WgXcQ-137.mp4",
            "video/mp4; codecs=\"avc1\"",
        ),
        (
            streams["subtitles"][1]["formats"][0]["download"].clone(),
            "dQw4w9WgXcQ-fr.vtt",
            "text/vtt; charset=utf-8",
        ),
    ];
    for (selection, filename, content_type) in cases {
        assert!(
            serde_json::from_value::<DownloadSelection>(selection.clone()).is_ok(),
            "{selection}"
        );
        let response = request(service.clone(), "dQw4w9WgXcQ", selection, None).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers()[header::CONTENT_DISPOSITION],
            format!("attachment; filename=\"{filename}\"")
        );
        assert_eq!(response.headers()[header::CONTENT_TYPE], content_type);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let bytes = to_bytes(response.into_body(), 1024).await.unwrap();
        assert!(!bytes.is_empty());
        assert_eq!(service.slots.available_permits(), 4);
    }
    let calls = state.calls.lock().unwrap();
    let (path, headers) = calls
        .iter()
        .find(|(path, _)| path.starts_with("/subtitle"))
        .unwrap();
    assert!(path.contains("tlang=fr") && path.contains("fmt=vtt") && path.contains("sig=a%2Bb"));
    assert!(
        headers[header::USER_AGENT]
            .to_str()
            .unwrap()
            .contains("com.google.ios.youtube")
    );
    assert_eq!(headers[header::REFERER], "https://www.youtube.com/");
    assert_eq!(headers[header::ACCEPT_ENCODING], "identity");
    assert!(!headers.contains_key("authorization"));
}

#[tokio::test]
async fn distinguishes_audio_languages_with_the_same_format_id() {
    let (service, state, _server) = setup("/media").await;
    let mut selection = audio();
    selection["audio_track_id"] = json!("es.1");
    let response = request(service, "dQw4w9WgXcQ", selection, None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        state
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(path, _)| path == "/media?lang=es")
    );
}

#[tokio::test]
async fn forwards_single_ranges_and_preserves_partial_and_unsatisfiable_responses() {
    let (service, _, _server) = setup("/media").await;
    let response = request(service.clone(), "dQw4w9WgXcQ", audio(), Some("bytes=2-5")).await;
    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes 2-5/10");
    assert_eq!(response.headers()[header::CONTENT_LENGTH], "4");
    assert_eq!(to_bytes(response.into_body(), 1024).await.unwrap(), "2345");
    let response = request(service.clone(), "dQw4w9WgXcQ", audio(), Some("bytes=999-")).await;
    assert_eq!(response.status(), StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes */10");
    assert_eq!(service.slots.available_permits(), 4);
    // An origin may ignore ranges; do not mislabel the full body as 206.
    let response = request(service, "dQw4w9WgXcQ", audio(), Some("bytes=0-1")).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap(),
        "0123456789"
    );
}

#[tokio::test]
async fn invalid_input_never_contacts_upstream_and_unknown_selection_is_404() {
    let (service, state, _server) = setup("/media").await;
    for range in [
        "bytes=",
        "bytes=-",
        "bytes=-0",
        "bytes=3-1",
        "bytes=0-1,3-4",
        "items=0-1",
        "bytes=+1-2",
        "bytes=0-18446744073709551616",
    ] {
        assert_eq!(
            request(service.clone(), "dQw4w9WgXcQ", audio(), Some(range))
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    for selection in [
        json!({"type": "url", "url": "http://localhost/"}),
        json!({"type": "audio", "format_id": "140", "source_client": "IOS", "url": "http://localhost/"}),
        json!({"type": "audio", "format_id": "", "source_client": "IOS"}),
    ] {
        assert_eq!(
            request(service.clone(), "dQw4w9WgXcQ", selection, None)
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
    }
    assert_eq!(
        request(service.clone(), "bad", audio(), None)
            .await
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert!(state.calls.lock().unwrap().is_empty());
    let mut missing = audio();
    missing["format_id"] = json!("999");
    let response = request(service, "dQw4w9WgXcQ", missing, None).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert!(
        state
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|(path, _)| path == "player")
    );
}

#[tokio::test]
async fn validates_redirect_destinations_and_sanitizes_upstream_failures() {
    let (service, _, _server) = setup("/redirect").await;
    let response = request(service, "dQw4w9WgXcQ", audio(), None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), 1024).await.unwrap(),
        "0123456789"
    );
    for path in ["/badredirect", "/failure"] {
        let (service, _, _server) = setup(path).await;
        let response = request(service.clone(), "dQw4w9WgXcQ", audio(), None).await;
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            if path == "/failure" {
                json!({"error": "YouTube media server rejected the download", "upstream_status": 403})
            } else {
                json!({"error": "Unable to download the selected stream"})
            }
        );
        assert_eq!(service.slots.available_permits(), 4);
    }
}

#[tokio::test]
async fn holds_download_slots_until_response_consumption_or_cancellation() {
    let (service, state, _server) = setup("/media").await;
    let mut responses = Vec::new();
    for _ in 0..4 {
        responses.push(request(service.clone(), "dQw4w9WgXcQ", audio(), None).await);
    }
    assert_eq!(service.slots.available_permits(), 0);
    let calls = state.calls.lock().unwrap().len();
    assert_eq!(
        request(service.clone(), "dQw4w9WgXcQ", audio(), None)
            .await
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(state.calls.lock().unwrap().len(), calls);
    responses.pop();
    assert_eq!(service.slots.available_permits(), 1);
    drop(responses);
    assert_eq!(service.slots.available_permits(), 4);
}

#[tokio::test]
async fn idle_timeout_is_504_before_headers_and_later_failure_aborts_body() {
    let (mut service, _, _server) = setup("/slow").await;
    service.client = Client::builder()
        .read_timeout(Duration::from_millis(20))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    assert_eq!(
        request(service.clone(), "dQw4w9WgXcQ", audio(), None)
            .await
            .status(),
        StatusCode::GATEWAY_TIMEOUT
    );
    assert_eq!(service.slots.available_permits(), 4);
    let (service, state, _server) = setup("/broken").await;
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        request(service.clone(), "dQw4w9WgXcQ", audio(), None),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    // Headers were returned while the rest of the upstream body was pending.
    assert_eq!(service.slots.available_permits(), 3);
    state.fail_body.notify_one();
    assert!(to_bytes(response.into_body(), 1024).await.is_err());
    assert_eq!(service.slots.available_permits(), 4);
}

#[tokio::test]
async fn only_expected_youtube_https_media_hosts_are_downloaded() {
    let (mut service, _, _server) = setup("/media").await;
    service.test_origin = None;
    for (url, subtitle, expected) in [
        (
            "https://rr1---sn.example.googlevideo.com/videoplayback",
            false,
            true,
        ),
        ("https://www.youtube.com/api/timedtext?sig=x", true, true),
        ("https://video.google.com/api/timedtext", true, true),
        ("http://rr1.googlevideo.com/videoplayback", false, false),
        (
            "https://rr1.googlevideo.com:8443/videoplayback",
            false,
            false,
        ),
        ("https://googlevideo.com.evil.example/file", false, false),
        ("https://evilgooglevideo.com/file", false, false),
        (
            "https://user:password@rr1.googlevideo.com/file",
            false,
            false,
        ),
        ("https://127.0.0.1/file", false, false),
        ("https://www.youtube.com/redirect", true, false),
        ("https://www.youtube.com/api/timedtext", false, false),
    ] {
        assert_eq!(
            service.allowed_url(&Url::parse(url).unwrap(), subtitle),
            expected,
            "{url}"
        );
    }
    let response = request(service, "dQw4w9WgXcQ", audio(), None).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
}
