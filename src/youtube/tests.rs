use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Query, State},
    http::{Request, StatusCode},
    routing::get,
};
use serde_json::{Value, json};
use tower::ServiceExt;

use super::*;

#[test]
fn normalizes_handles_and_urls() {
    for input in [
        "@Jes%C3%BAsGMaestro",
        "@JesúsGMaestro",
        "https://www.youtube.com/@Jes%C3%BAsGMaestro",
        "https://youtube.com/@JesúsGMaestro/",
        " https://m.youtube.com/@Jes%C3%BAsGMaestro?feature=shared ",
    ] {
        assert_eq!(normalize_handle(input).unwrap(), "@JesúsGMaestro");
    }
}

#[test]
fn rejects_invalid_handles_and_urls() {
    for input in [
        "",
        "@",
        "JesúsGMaestro",
        "@bad handle",
        "@bad/handle",
        "@bad%2Fhandle",
        "@bad%FF",
        "@bad%ZZ",
        "@bad%252Fhandle",
        "https://example.com/@handle",
        "https://youtube.com.evil.test/@handle",
        "https://youtube.com/channel/id",
        "https://youtube.com/@handle/videos",
        "https://user@youtube.com/@handle",
        "ftp://youtube.com/@handle",
    ] {
        assert!(normalize_handle(input).is_err(), "accepted {input}");
    }
}

type RecordedRequests = Arc<Mutex<Vec<(String, HashMap<String, String>)>>>;

#[derive(Clone)]
struct MockState {
    requests: RecordedRequests,
    channel_body: Value,
    pages: HashMap<String, (StatusCode, Value)>,
    video_details: HashMap<String, Value>,
    details_override: Option<(StatusCode, Value)>,
    search_pages: HashMap<String, (StatusCode, Value)>,
    playlists_pages: HashMap<String, (StatusCode, Value)>,
}

impl MockState {
    fn new(pages: HashMap<String, (StatusCode, Value)>) -> Self {
        let video_details = pages
            .values()
            .flat_map(|(_, page)| {
                page["items"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|item| {
                        let id = item["contentDetails"]["videoId"].as_str()?;
                        Some((id.to_owned(), details(id, "PT10M", Some((1920, 1080)))))
                    })
            })
            .collect();
        Self {
            requests: Arc::default(),
            channel_body: json!({"items": [{
                "id": "UC_example",
                "contentDetails": {"relatedPlaylists": {"uploads": "UU_example"}}
            }]}),
            pages,
            video_details,
            details_override: None,
            search_pages: HashMap::new(),
            playlists_pages: HashMap::new(),
        }
    }
}

async fn search_results(
    State(state): State<MockState>,
    Query(query): Query<HashMap<String, String>>,
) -> (StatusCode, Json<Value>) {
    let token = query.get("pageToken").cloned().unwrap_or_default();
    state
        .requests
        .lock()
        .unwrap()
        .push(("search".into(), query));
    let (status, body) = state
        .search_pages
        .get(&token)
        .expect("unexpected search page");
    (*status, Json(body.clone()))
}

async fn playlist_results(
    State(state): State<MockState>,
    Query(query): Query<HashMap<String, String>>,
) -> (StatusCode, Json<Value>) {
    let token = query.get("pageToken").cloned().unwrap_or_default();
    state
        .requests
        .lock()
        .unwrap()
        .push(("playlists".into(), query));
    let (status, body) = state
        .playlists_pages
        .get(&token)
        .expect("unexpected playlists page");
    (*status, Json(body.clone()))
}

async fn video_details(
    State(state): State<MockState>,
    Query(query): Query<HashMap<String, String>>,
) -> (StatusCode, Json<Value>) {
    state
        .requests
        .lock()
        .unwrap()
        .push(("videos".into(), query.clone()));
    if let Some((status, body)) = state.details_override {
        return (status, Json(body));
    }
    let ids: Vec<_> = query["id"].split(',').collect();
    assert!(ids.len() <= 50);
    let items: Vec<_> = ids
        .into_iter()
        .rev()
        .filter_map(|id| state.video_details.get(id).cloned())
        .collect();
    (StatusCode::OK, Json(json!({"items": items})))
}

async fn channels(
    State(state): State<MockState>,
    Query(query): Query<HashMap<String, String>>,
) -> Json<Value> {
    state
        .requests
        .lock()
        .unwrap()
        .push(("channels".into(), query));
    Json(state.channel_body)
}

async fn playlist(
    State(state): State<MockState>,
    Query(query): Query<HashMap<String, String>>,
) -> (StatusCode, Json<Value>) {
    let token = query.get("pageToken").cloned().unwrap_or_default();
    state
        .requests
        .lock()
        .unwrap()
        .push(("playlistItems".into(), query));
    let (status, body) = state.pages.get(&token).expect("unexpected page token");
    (*status, Json(body.clone()))
}

struct MockServer(tokio::task::JoinHandle<()>);

impl Drop for MockServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn setup(state: MockState) -> (Router, MockServer) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
    let mock = Router::new()
        .route("/channels", get(channels))
        .route("/playlistItems", get(playlist))
        .route("/videos", get(video_details))
        .route("/search", get(search_results))
        .route("/playlists", get(playlist_results))
        .with_state(state);
    let server = MockServer(tokio::spawn(async move {
        axum::serve(listener, mock).await.unwrap();
    }));
    let mut client = YouTubeClient::new("test-secret-key".into()).unwrap();
    client.base_url = base_url;
    (crate::app(client), server)
}

async fn request(app: Router, channel: Option<&str>) -> (StatusCode, Value) {
    request_with_filters(app, channel, &[]).await
}

async fn request_with_filters(
    app: Router,
    channel: Option<&str>,
    filters: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut url = Url::parse("http://localhost/videos").unwrap();
    if let Some(channel) = channel {
        url.query_pairs_mut().append_pair("channel", channel);
    }
    url.query_pairs_mut().extend_pairs(filters.iter().copied());
    let response = app
        .oneshot(
            Request::builder()
                .uri(url.as_str())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

fn item(id: &str) -> Value {
    json!({
        "id": format!("playlist-item-{id}"),
        "snippet": {"title": format!("Video {id}"), "description": "Description"},
        "contentDetails": {"videoId": id, "videoPublishedAt": "2026-01-01T12:00:00Z"}
    })
}

fn details(id: &str, duration: &str, dimensions: Option<(u64, u64)>) -> Value {
    let player = dimensions.map(|(width, height)| json!({"embedWidth": width.to_string(), "embedHeight": height.to_string()}));
    json!({"id": id, "contentDetails": {"duration": duration}, "player": player})
}

#[tokio::test]
async fn aggregates_every_page_and_decodes_unicode_input() {
    let state = MockState::new(HashMap::from([
        (
            "".into(),
            (
                StatusCode::OK,
                json!({"items": [item("a")], "nextPageToken": "page+2/="}),
            ),
        ),
        (
            "page+2/=".into(),
            (
                StatusCode::OK,
                json!({"items": [], "nextPageToken": "page3"}),
            ),
        ),
        (
            "page3".into(),
            (StatusCode::OK, json!({"items": [item("b")]})),
        ),
    ]));
    let (app, _server) = setup(state.clone()).await;
    for input in [
        "https://www.youtube.com/@Jes%C3%BAsGMaestro",
        "@Jes%C3%BAsGMaestro",
    ] {
        let (status, body) = request(app.clone(), Some(input)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["channel_id"], "UC_example");
        assert_eq!(body["playlist_id"], "UU_example");
        assert_eq!(body["total"], 2);
        assert_eq!(body["shorts_detection"], "estimated");
        assert_eq!(body["videos"][0]["duration_seconds"], 600);
        assert_eq!(body["videos"][0]["duration"], "10:00");
        assert_eq!(body["videos"][0]["is_short"], false);
        assert_eq!(body["videos"][0]["id"], "a");
        assert_eq!(body["videos"][1]["id"], "b");
        assert_eq!(
            body["videos"][0]["url"],
            "https://www.youtube.com/watch?v=a"
        );
        assert_eq!(body["videos"][0]["published_at"], "2026-01-01T12:00:00Z");
    }
    let recorded = state.requests.lock().unwrap();
    let requests: Vec<_> = recorded
        .iter()
        .filter(|(resource, _)| resource != "videos")
        .collect();
    assert_eq!(requests.len(), 8);
    for batch in requests.chunks(4) {
        assert_eq!(batch[0].1["forHandle"], "@JesúsGMaestro");
        assert_eq!(batch[0].1["part"], "contentDetails");
        assert!(!batch[1].1.contains_key("pageToken"));
        assert_eq!(batch[2].1["pageToken"], "page+2/=");
        assert_eq!(batch[3].1["pageToken"], "page3");
        for (_, query) in &batch[1..] {
            assert_eq!(query["playlistId"], "UU_example");
            assert_eq!(query["maxResults"], "50");
            assert_eq!(query["key"], "test-secret-key");
        }
    }
}

#[tokio::test]
async fn empty_channel_returns_empty_list() {
    let state = MockState::new(HashMap::from([(
        "".into(),
        (StatusCode::OK, json!({"items": []})),
    )]));
    let (app, _server) = setup(state).await;
    let (status, body) = request(app, Some("@example")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 0);
    assert_eq!(body["videos"], json!([]));
}

#[tokio::test]
async fn unknown_channel_returns_404_without_listing_videos() {
    let mut state = MockState::new(HashMap::new());
    state.channel_body = json!({"items": []});
    let (app, _server) = setup(state.clone()).await;
    let (status, _) = request(app, Some("@missing")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(state.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn invalid_input_returns_400_without_upstream_requests() {
    let state = MockState::new(HashMap::new());
    let (app, _server) = setup(state.clone()).await;
    for input in [None, Some(""), Some("https://example.com/@handle")] {
        let (status, body) = request(app.clone(), input).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body["error"].is_string());
    }
    assert!(state.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn later_page_error_does_not_return_partial_results_or_secrets() {
    let state = MockState::new(HashMap::from([
        (
            "".into(),
            (
                StatusCode::OK,
                json!({"items": [item("a")], "nextPageToken": "next"}),
            ),
        ),
        (
            "next".into(),
            (
                StatusCode::FORBIDDEN,
                json!({"error": "quota exceeded test-secret-key"}),
            ),
        ),
    ]));
    let (app, _server) = setup(state).await;
    let (status, body) = request(app, Some("@example")).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(body, json!({"error": "YouTube API request failed"}));
}

#[tokio::test]
async fn repeated_pagination_token_fails_instead_of_looping() {
    let state = MockState::new(HashMap::from([
        (
            "".into(),
            (
                StatusCode::OK,
                json!({"items": [], "nextPageToken": "repeat"}),
            ),
        ),
        (
            "repeat".into(),
            (
                StatusCode::OK,
                json!({"items": [], "nextPageToken": "repeat"}),
            ),
        ),
    ]));
    let (app, _server) = setup(state.clone()).await;
    let (status, _) = request(app, Some("@example")).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert_eq!(state.requests.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn malformed_upstream_response_fails() {
    let state = MockState::new(HashMap::from([(
        "".into(),
        (StatusCode::OK, json!({"items": [{"unexpected": true}]})),
    )]));
    let (app, _server) = setup(state).await;
    let (status, _) = request(app, Some("@example")).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
}

#[test]
fn parses_youtube_durations_and_rejects_malformed_or_overflowing_values() {
    for (input, expected) in [
        ("PT0S", 0),
        ("PT59S", 59),
        ("PT3M", 180),
        ("PT1H2M3S", 3723),
        ("P2DT3H4M5S", 183_845),
        ("P1D", 86_400),
    ] {
        assert_eq!(parse_duration_seconds(input).unwrap(), expected, "{input}");
    }
    for input in [
        "",
        "P",
        "PT",
        "P1DT",
        "PT1M2H",
        "PT1M1M",
        "P1Y",
        "P1M",
        "PT-1S",
        "PT1.5S",
        "PT1Sx",
        "PT18446744073709551615H",
    ] {
        assert!(parse_duration_seconds(input).is_err(), "{input}");
    }
}

#[test]
fn shorts_estimate_uses_duration_aspect_ratio_and_rollout_dates() {
    let vertical = Player {
        embed_width: Some("608".into()),
        embed_height: Some("1080".into()),
    };
    let square = Player {
        embed_width: Some("1080".into()),
        embed_height: Some("1080".into()),
    };
    let horizontal = Player {
        embed_width: Some("1920".into()),
        embed_height: Some("1080".into()),
    };
    let unknown = Player {
        embed_width: None,
        embed_height: None,
    };
    assert_eq!(estimate_short(Some(60), Some(&vertical), None), Some(true));
    assert_eq!(estimate_short(Some(60), Some(&square), None), Some(true));
    assert_eq!(
        estimate_short(Some(60), Some(&horizontal), None),
        Some(false)
    );
    assert_eq!(estimate_short(Some(181), None, None), Some(false));
    assert_eq!(
        estimate_short(Some(180), Some(&vertical), Some("2025-12-08T00:00:00Z")),
        Some(true)
    );
    assert_eq!(
        estimate_short(Some(61), Some(&vertical), Some("2024-10-14T23:59:59Z")),
        Some(false)
    );
    for date in [
        None,
        Some("invalid"),
        Some("2024-10-15T00:00:00Z"),
        Some("2025-12-07T23:59:59Z"),
    ] {
        assert_eq!(estimate_short(Some(120), Some(&vertical), date), None);
    }
    for duration in [None, Some(0)] {
        assert_eq!(estimate_short(duration, Some(&vertical), None), None);
    }
    assert_eq!(estimate_short(Some(60), Some(&unknown), None), None);
    assert_eq!(estimate_short(Some(60), None, None), None);
}

fn filtered_state() -> MockState {
    let mut state = MockState::new(HashMap::from([
        (
            "".into(),
            (
                StatusCode::OK,
                json!({"items": [item("short"), item("first-match")], "nextPageToken": "page2"}),
            ),
        ),
        (
            "page2".into(),
            (
                StatusCode::OK,
                json!({"items": [item("unknown"), item("second-match"), item("third-match"), item("too-long")], "nextPageToken": "must-not-fetch"}),
            ),
        ),
    ]));
    state.video_details = HashMap::from([
        ("short".into(), details("short", "PT1M", Some((608, 1080)))),
        (
            "first-match".into(),
            details("first-match", "PT2M", Some((1920, 1080))),
        ),
        ("unknown".into(), details("unknown", "PT2M", None)),
        (
            "second-match".into(),
            details("second-match", "PT3M", Some((1920, 1080))),
        ),
        (
            "third-match".into(),
            details("third-match", "PT3M", Some((1920, 1080))),
        ),
        (
            "too-long".into(),
            details("too-long", "PT10M", Some((1920, 1080))),
        ),
    ]);
    state
}

#[tokio::test]
async fn filters_then_skips_and_takes_matches_across_upstream_pages() {
    let state = filtered_state();
    let (app, _server) = setup(state.clone()).await;
    let (status, body) = request_with_filters(
        app,
        Some("@example"),
        &[
            ("is_short", "false"),
            ("min_duration", "120"),
            ("max_duration", "180"),
            ("skip", "1"),
            ("top", "2"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 2);
    assert_eq!(body["videos"][0]["id"], "second-match");
    assert_eq!(body["videos"][1]["id"], "third-match");
    let requests = state.requests.lock().unwrap();
    assert_eq!(requests.len(), 5);
    for (_, query) in requests.iter().filter(|(resource, _)| resource == "videos") {
        assert_eq!(query["part"], "contentDetails,player");
        assert_eq!(query["maxHeight"], "1080");
        assert_eq!(query["maxWidth"], "1920");
    }
}

#[tokio::test]
async fn shorts_filter_and_independent_duration_bounds_work() {
    for filters in [
        vec![("is_short", "true"), ("top", "1")],
        vec![("max_duration", "60"), ("top", "1")],
        vec![("min_duration", "60"), ("top", "1")],
    ] {
        let (app, _server) = setup(filtered_state()).await;
        let (status, body) = request_with_filters(app, Some("@example"), &filters).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["videos"][0]["id"], "short");
        assert_eq!(body["videos"][0]["is_short"], true);
    }
}

#[tokio::test]
async fn rejects_invalid_filters_before_calling_upstream() {
    let state = MockState::new(HashMap::new());
    let (app, _server) = setup(state.clone()).await;
    for filters in [
        vec![("min_duration", "100"), ("max_duration", "99")],
        vec![("min_duration", "-1")],
        vec![("max_duration", "1.5")],
        vec![("is_short", "yes")],
        vec![("top", "-1")],
        vec![("skip", "abc")],
        vec![("skip", "18446744073709551616")],
    ] {
        let (status, body) = request_with_filters(app.clone(), Some("@example"), &filters).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{filters:?}: {body}");
    }
    assert!(state.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn zero_top_only_resolves_channel() {
    let state = MockState::new(HashMap::new());
    let (app, _server) = setup(state.clone()).await;
    let (status, body) = request_with_filters(app, Some("@example"), &[("top", "0")]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["videos"], json!([]));
    assert_eq!(state.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn skip_beyond_matches_returns_empty_list() {
    let state = MockState::new(HashMap::from([(
        "".into(),
        (StatusCode::OK, json!({"items": [item("a")]})),
    )]));
    let (app, _server) = setup(state).await;
    let (status, body) = request_with_filters(app, Some("@example"), &[("skip", "2")]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["videos"], json!([]));
}

#[tokio::test]
async fn unavailable_metadata_remains_unknown_and_is_excluded_by_filters() {
    let mut state = MockState::new(HashMap::from([(
        "".into(),
        (StatusCode::OK, json!({"items": [item("missing")]})),
    )]));
    state.video_details.clear();
    let (app, _server) = setup(state).await;
    let (status, body) = request(app.clone(), Some("@example")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["videos"][0]["duration_seconds"], Value::Null);
    assert_eq!(body["videos"][0]["duration"], Value::Null);
    assert_eq!(body["videos"][0]["is_short"], Value::Null);
    for filters in [
        vec![("is_short", "true")],
        vec![("is_short", "false")],
        vec![("min_duration", "0")],
        vec![("max_duration", "180")],
    ] {
        let (status, body) = request_with_filters(app.clone(), Some("@example"), &filters).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["videos"], json!([]));
    }
}

#[tokio::test]
async fn batches_metadata_in_groups_of_at_most_fifty_and_preserves_order() {
    let items: Vec<_> = (0..51).map(|n| item(&format!("v{n}"))).collect();
    let state = MockState::new(HashMap::from([
        (
            "".into(),
            (
                StatusCode::OK,
                json!({"items": &items[..50], "nextPageToken": "next"}),
            ),
        ),
        (
            "next".into(),
            (StatusCode::OK, json!({"items": &items[50..]})),
        ),
    ]));
    let (app, _server) = setup(state.clone()).await;
    let (status, body) = request(app, Some("@example")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], 51);
    assert_eq!(body["videos"][0]["id"], "v0");
    assert_eq!(body["videos"][50]["id"], "v50");
    let requests = state.requests.lock().unwrap();
    let sizes: Vec<_> = requests
        .iter()
        .filter(|(resource, _)| resource == "videos")
        .map(|(_, query)| query["id"].split(',').count())
        .collect();
    assert_eq!(sizes, vec![50, 1]);
}

#[tokio::test]
async fn failed_or_malformed_metadata_request_fails_whole_response() {
    for (status, body) in [
        (StatusCode::FORBIDDEN, json!({"error": "test-secret-key"})),
        (
            StatusCode::OK,
            json!({"items": [{"id": "a", "contentDetails": {"duration": "invalid"}}]}),
        ),
    ] {
        let mut state = MockState::new(HashMap::from([(
            "".into(),
            (StatusCode::OK, json!({"items": [item("a")]})),
        )]));
        state.details_override = Some((status, body));
        let (app, _server) = setup(state).await;
        let (status, body) = request(app, Some("@example")).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body, json!({"error": "YouTube API request failed"}));
    }
}

mod search;

fn with_playlist_source(mut state: MockState) -> MockState {
    state.playlists_pages.insert(
        "".into(),
        (
            StatusCode::OK,
            json!({"items": [{
                "id": "PL_example", "snippet": {"channelId": "UC_playlist_owner"}
            }]}),
        ),
    );
    state
}

#[tokio::test]
async fn playlist_source_preserves_filters_and_offsets_across_pages() {
    let state = with_playlist_source(filtered_state());
    let (app, _server) = setup(state.clone()).await;
    let (status, body) = request_with_filters(
        app,
        None,
        &[
            ("playlist_id", "PL_example"),
            ("is_short", "false"),
            ("min_duration", "120"),
            ("max_duration", "180"),
            ("skip", "1"),
            ("top", "2"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["playlist_id"], "PL_example");
    assert_eq!(body["channel_id"], "UC_playlist_owner");
    assert_eq!(body["total"], 2);
    assert_eq!(body["videos"][0]["id"], "second-match");
    assert_eq!(body["videos"][1]["id"], "third-match");
    assert_eq!(body["videos"][0]["duration"], "03:00");
    let requests = state.requests.lock().unwrap();
    assert_eq!(requests.len(), 5);
    assert_eq!(requests[0].0, "playlists");
    assert_eq!(requests[0].1["id"], "PL_example");
    assert_eq!(requests[0].1["part"], "snippet");
    assert!(!requests.iter().any(|(resource, _)| resource == "channels"));
    let pages: Vec<_> = requests
        .iter()
        .filter(|(resource, _)| resource == "playlistItems")
        .collect();
    assert_eq!(pages.len(), 2);
    for (_, query) in &pages {
        assert_eq!(query["playlistId"], "PL_example");
    }
    assert_eq!(pages[1].1["pageToken"], "page2");
}

#[tokio::test]
async fn playlist_source_follows_all_upstream_pages_without_a_limit() {
    let state = with_playlist_source(MockState::new(HashMap::from([
        (
            "".into(),
            (
                StatusCode::OK,
                json!({"items": [item("a")], "nextPageToken": "page2"}),
            ),
        ),
        (
            "page2".into(),
            (
                StatusCode::OK,
                json!({"items": [], "nextPageToken": "page3"}),
            ),
        ),
        (
            "page3".into(),
            (StatusCode::OK, json!({"items": [item("b")]})),
        ),
    ])));
    let (app, _server) = setup(state).await;
    let (status, body) = request_with_filters(app, None, &[("playlist_id", "PL_example")]).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 2);
    assert_eq!(body["videos"][0]["id"], "a");
    assert_eq!(body["videos"][1]["id"], "b");
}

#[tokio::test]
async fn video_source_validation_rejects_missing_conflicting_or_invalid_sources() {
    let state = MockState::new(HashMap::new());
    let (app, _server) = setup(state.clone()).await;
    for (channel, params) in [
        (None, vec![]),
        (Some("@example"), vec![("playlist_id", "PL_example")]),
        (None, vec![("playlist_id", "")]),
        (None, vec![("playlist_id", " ")]),
        (None, vec![("playlist_id", "PL_one,PL_two")]),
        (
            None,
            vec![(
                "playlist_id",
                "https://youtube.com/playlist?list=PL_example",
            )],
        ),
        (
            None,
            vec![
                ("playlist_id", "PL_example"),
                ("min_duration", "2"),
                ("max_duration", "1"),
            ],
        ),
    ] {
        let (status, body) = request_with_filters(app.clone(), channel, &params).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{params:?}: {body}");
    }
    assert!(state.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn missing_playlist_returns_404_for_empty_lookup_or_upstream_not_found() {
    for (status, body) in [
        (StatusCode::OK, json!({"items": []})),
        (StatusCode::NOT_FOUND, json!({"error": "test-secret-key"})),
    ] {
        let mut state = MockState::new(HashMap::new());
        state.playlists_pages.insert("".into(), (status, body));
        let (app, _server) = setup(state.clone()).await;
        let (status, body) =
            request_with_filters(app, None, &[("playlist_id", "PL_missing")]).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body, json!({"error": "YouTube playlist not found"}));
        assert_eq!(state.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn empty_playlist_and_zero_top_return_empty_results_with_source_metadata() {
    for params in [
        vec![("playlist_id", "PL_example")],
        vec![("playlist_id", "PL_example"), ("top", "0")],
    ] {
        let state = with_playlist_source(MockState::new(HashMap::from([(
            "".into(),
            (StatusCode::OK, json!({"items": []})),
        )])));
        let (app, _server) = setup(state.clone()).await;
        let (status, body) = request_with_filters(app, None, &params).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["playlist_id"], "PL_example");
        assert_eq!(body["channel_id"], "UC_playlist_owner");
        assert_eq!(body["total"], 0);
        assert_eq!(body["videos"], json!([]));
        assert_eq!(
            state.requests.lock().unwrap().len(),
            if params.len() == 2 { 1 } else { 2 }
        );
    }
}

#[tokio::test]
async fn playlist_video_request_handles_disappearance_and_later_page_errors() {
    for (status, expected) in [
        (StatusCode::NOT_FOUND, StatusCode::NOT_FOUND),
        (StatusCode::FORBIDDEN, StatusCode::BAD_GATEWAY),
    ] {
        let state = with_playlist_source(MockState::new(HashMap::from([
            (
                "".into(),
                (
                    StatusCode::OK,
                    json!({"items": [item("a")], "nextPageToken": "page2"}),
                ),
            ),
            (
                "page2".into(),
                (status, json!({"error": "test-secret-key"})),
            ),
        ])));
        let (app, _server) = setup(state).await;
        let (status, body) =
            request_with_filters(app, None, &[("playlist_id", "PL_example")]).await;
        assert_eq!(status, expected);
        assert!(body["videos"].is_null());
        assert!(!body.to_string().contains("test-secret-key"));
    }
}
