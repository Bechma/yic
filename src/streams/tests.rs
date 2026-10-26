use super::*;
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    routing::post,
};
use std::sync::Mutex;
use tower::ServiceExt;

type Calls = Arc<Mutex<Vec<(HeaderMap, Value)>>>;
#[derive(Clone)]
struct MockPlayer {
    ios: (StatusCode, String),
    tv: (StatusCode, String),
    delay: Duration,
    calls: Calls,
}
impl MockPlayer {
    fn new(ios: Value, tv: Value) -> Self {
        Self {
            ios: (StatusCode::OK, ios.to_string()),
            tv: (StatusCode::OK, tv.to_string()),
            delay: Duration::ZERO,
            calls: Arc::default(),
        }
    }
}
async fn player(
    State(state): State<MockPlayer>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> (StatusCode, [(String, String); 1], String) {
    let ios = body["context"]["client"]["clientName"] == "IOS";
    state.calls.lock().unwrap().push((headers, body));
    tokio::time::sleep(state.delay).await;
    let (status, response) = if ios { state.ios } else { state.tv };
    (
        status,
        [("content-type".into(), "application/json".into())],
        response,
    )
}
struct Server(tokio::task::JoinHandle<()>);
impl Drop for Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn setup(state: MockPlayer) -> (StreamExtractor, Server) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = Router::new()
        .route("/player", post(player))
        .with_state(state);
    let server = Server(tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    }));
    let extractor =
        StreamExtractor::for_test(Url::parse(&format!("http://{address}/player")).unwrap());
    (extractor, server)
}
async fn request(extractor: StreamExtractor, id: &str) -> (StatusCode, Value) {
    let app = crate::app_with_stream_extractor(
        crate::YouTubeClient::new("secret-api-key".into()).unwrap(),
        extractor,
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/videos/{id}/streams"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}
fn format(itag: u32, mime: &str, path: &str) -> Value {
    json!({"itag": itag, "mimeType": mime, "url": format!("https://media.example/{path}"), "bitrate": 128000, "contentLength": "123456", "approxDurationMs": "123000"})
}
fn playable(formats: Vec<Value>) -> Value {
    json!({"playabilityStatus": {"status": "OK"}, "videoDetails": {"videoId": "dQw4w9WgXcQ", "title": "Example", "lengthSeconds": "123"}, "streamingData": {"expiresInSeconds": "21600", "adaptiveFormats": formats}})
}
fn blocked() -> Value {
    json!({"playabilityStatus": {"status": "LOGIN_REQUIRED", "reason": "secret upstream diagnostic"}})
}

#[tokio::test]
async fn combines_clients_formats_manifests_and_all_caption_languages_without_subprocesses() {
    let video = format(137, "video/mp4; codecs=\"avc1.640028\"", "video");
    let mut ios = playable(vec![
        format(18, "video/mp4; codecs=\"avc1, mp4a\"", "muxed"),
        video.clone(),
        video,
        format(140, "audio/mp4; codecs=\"mp4a.40.2\"", "audio-en"),
        json!({"itag": 251, "url": "https://media.example/audio-es", "mimeType": "audio/webm; codecs=\"opus\"", "audioTrack": {"id": "es.1", "displayName": "Español"}}),
    ]);
    ios["captions"] = json!({"playerCaptionsTracklistRenderer": {
        "captionTracks": [
            {"baseUrl": "https://captions.example/en?sig=a%2Bb%2Fc&fmt=old", "languageCode": "en", "name": {"simpleText": "English"}, "vssId": ".en", "isTranslatable": true},
            {"baseUrl": "https://captions.example/es", "languageCode": "es", "name": {"runs": [{"text": "Español"}]}, "vssId": "a.es", "kind": "asr", "isTranslatable": true}
        ],
        "translationLanguages": [{"languageCode": "fr", "languageName": {"simpleText": "French"}}]
    }});
    let mut tv = playable(vec![format(303, "video/webm; codecs=\"vp9\"", "video-hdr")]);
    tv["streamingData"]["expiresInSeconds"] = json!("3600");
    tv["streamingData"]["hlsManifestUrl"] = json!("https://media.example/master.m3u8");
    tv["streamingData"]["dashManifestUrl"] = json!("https://media.example/master.mpd");
    let state = MockPlayer::new(ios, tv);
    let (extractor, _server) = setup(state.clone()).await;
    let (status, body) = request(extractor, "dQw4w9WgXcQ").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["extractor"], "native_innertube");
    assert_eq!(body["coverage"], "native_client_formats");
    assert_eq!(body["urls_are_temporary"], true);
    assert_eq!(body["duration_seconds"], 123);
    assert_eq!(body["expires_in_seconds"], 3600);
    assert_eq!(body["video_streams"].as_array().unwrap().len(), 3);
    assert_eq!(body["video_streams"][0]["has_audio"], true);
    assert_eq!(body["video_streams"][1]["has_audio"], false);
    assert_eq!(body["video_streams"][1]["content_length"], 123456);
    assert_eq!(body["video_streams"][2]["source_client"], "TVHTML5");
    assert_eq!(body["audio_streams"].as_array().unwrap().len(), 2);
    assert_eq!(
        body["audio_streams"][1]["metadata"]["audioTrack"]["id"],
        "es.1"
    );
    assert_eq!(body["manifests"].as_array().unwrap().len(), 2);
    assert_eq!(body["manifests"][0]["kind"], "hls");
    assert_eq!(body["manifests"][1]["kind"], "dash");
    assert_eq!(body["subtitles"].as_array().unwrap().len(), 4);
    assert_eq!(body["subtitles"][0]["kind"], "manual");
    assert_eq!(body["subtitles"][0]["formats"].as_array().unwrap().len(), 6);
    assert_eq!(body["subtitles"][1]["is_translation"], true);
    assert_eq!(body["subtitles"][1]["language"], "fr");
    assert_eq!(body["subtitles"][1]["source_language"], "en");
    assert_eq!(body["subtitles"][2]["kind"], "automatic");
    let url = Url::parse(body["subtitles"][1]["formats"][0]["url"].as_str().unwrap()).unwrap();
    let pairs: BTreeMap<_, _> = url.query_pairs().collect();
    assert_eq!(pairs["sig"], "a+b/c");
    assert_eq!(pairs["fmt"], "vtt");
    assert_eq!(pairs["tlang"], "fr");
    assert_eq!(body["warnings"], json!([]));
    let calls = state.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    for (headers, body) in calls.iter() {
        assert!(!headers.contains_key("authorization"));
        assert!(!headers.contains_key("x-goog-api-key"));
        assert_eq!(body["videoId"], "dQw4w9WgXcQ");
        assert!(!body.to_string().contains("secret-api-key"));
    }
}

#[tokio::test]
async fn unavailable_client_does_not_discard_other_clients_supported_streams() {
    let state = MockPlayer::new(
        playable(vec![format(140, "audio/mp4; codecs=\"mp4a\"", "audio")]),
        blocked(),
    );
    let (extractor, _server) = setup(state).await;
    let (status, body) = request(extractor, "dQw4w9WgXcQ").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["audio_streams"].as_array().unwrap().len(), 1);
    assert_eq!(body["warnings"].as_array().unwrap().len(), 1);
    assert!(!body.to_string().contains("secret"));
}

#[tokio::test]
async fn unresolved_ciphers_n_urls_drm_and_sabr_are_reported_without_fake_playable_links() {
    let mut ios = playable(vec![
        format(140, "audio/mp4; codecs=\"mp4a\"", "audio"),
        json!({"itag": 137, "mimeType": "video/mp4", "signatureCipher": "s=encrypted&url=https%3A%2F%2Fmedia.example%2Fvideo"}),
        json!({"itag": 138, "mimeType": "video/mp4", "url": "https://media.example/video?n=obfuscated"}),
        json!({"itag": 139, "mimeType": "audio/mp4", "url": "https://media.example/drm", "drmFamilies": ["WIDEVINE"]}),
    ]);
    ios["streamingData"]["serverAbrStreamingUrl"] = json!("https://media.example/sabr");
    let (extractor, _server) = setup(MockPlayer::new(ios, blocked())).await;
    let (status, body) = request(extractor, "dQw4w9WgXcQ").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["video_streams"], json!([]));
    assert_eq!(body["audio_streams"].as_array().unwrap().len(), 1);
    assert_eq!(body["warnings"].as_array().unwrap().len(), 3);
    assert!(!body.to_string().contains("encrypted"));
}

#[test]
fn plaintext_signature_cipher_is_decoded_and_appended_without_modifying_other_parameters() {
    let mut cipher = Url::parse("https://cipher.invalid/").unwrap();
    cipher
        .query_pairs_mut()
        .append_pair("url", "https://media.example/audio?x=a%2Bb")
        .append_pair("sp", "sig")
        .append_pair("sig", "abc+/");
    let url = format_url(&json!({"signatureCipher": cipher.query().unwrap()})).unwrap();
    let pairs: BTreeMap<_, _> = url.query_pairs().collect();
    assert_eq!(pairs["sig"], "abc+/");
    assert_eq!(pairs["x"], "a+b");
    assert!(format_url(&json!({"url": "javascript:alert(1)"})).is_none());
    assert!(format_url(&json!({"url": "https://user:pass@media.example/stream"})).is_none());
}

#[tokio::test]
async fn invalid_video_id_and_busy_requests_do_not_call_youtube() {
    let state = MockPlayer::new(blocked(), blocked());
    let (extractor, _server) = setup(state.clone()).await;
    for id in ["short", "--bad=input", "1234567890%20", "1234567890%2F"] {
        assert_eq!(
            request(extractor.clone(), id).await.0,
            StatusCode::BAD_REQUEST
        );
    }
    let _permit = extractor.slots.acquire_many(4).await.unwrap();
    assert_eq!(
        request(extractor.clone(), "dQw4w9WgXcQ").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(state.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn blocked_malformed_and_wrong_video_responses_fail_without_exposing_upstream_data() {
    for response in [
        blocked(),
        json!({"unexpected": true}),
        json!({"playabilityStatus": {"status": "OK"}, "videoDetails": {"videoId": "wrong-video"}, "streamingData": {"adaptiveFormats": []}}),
    ] {
        let (extractor, _server) = setup(MockPlayer::new(response.clone(), response)).await;
        let (status, body) = request(extractor, "dQw4w9WgXcQ").await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(
            body,
            json!({"error": "Unable to extract streams for this video"})
        );
    }
}

#[tokio::test]
async fn timeout_returns_504_and_releases_extraction_capacity() {
    let mut state = MockPlayer::new(blocked(), blocked());
    state.delay = Duration::from_millis(200);
    let (mut extractor, _server) = setup(state).await;
    extractor.timeout = Duration::from_millis(20);
    assert_eq!(
        request(extractor.clone(), "dQw4w9WgXcQ").await.0,
        StatusCode::GATEWAY_TIMEOUT
    );
    assert_eq!(extractor.slots.available_permits(), 4);
}

#[tokio::test]
async fn http_failures_and_non_json_responses_are_sanitized() {
    let mut state = MockPlayer::new(blocked(), blocked());
    state.ios = (StatusCode::FORBIDDEN, "private server response".into());
    state.tv = (StatusCode::OK, "not JSON".into());
    let (extractor, _server) = setup(state).await;
    let (status, body) = request(extractor, "dQw4w9WgXcQ").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(!body.to_string().contains("private"));
}
