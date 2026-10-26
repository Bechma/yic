use super::*;

async fn search_request(app: Router, path: &str, params: &[(&str, &str)]) -> (StatusCode, Value) {
    let mut url = Url::parse(&format!("http://localhost{path}")).unwrap();
    url.query_pairs_mut().extend_pairs(params.iter().copied());
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

fn snippet() -> Value {
    json!({"title": "Literatura", "description": "Descripción", "channelId": "UC_example", "channelTitle": "Jesús G. Maestro", "publishedAt": "2026-01-01T12:00:00Z"})
}

fn result(kind: &str, id: &str) -> Value {
    json!({"id": {kind: id}, "snippet": snippet()})
}

#[tokio::test]
async fn channel_search_preserves_unicode_and_exposes_upstream_pagination() {
    let mut state = MockState::new(HashMap::new());
    state.search_pages = HashMap::from([
        (
            "".into(),
            (
                StatusCode::OK,
                json!({"items": [result("channelId", "UC_example")], "nextPageToken": "next+/="}),
            ),
        ),
        (
            "next+/=".into(),
            (
                StatusCode::OK,
                json!({"items": [], "prevPageToken": "previous"}),
            ),
        ),
    ]);
    let (app, _server) = setup(state.clone()).await;
    let (status, body) = search_request(
        app.clone(),
        "/channels/search",
        &[("q", " Jesús G. Maestro "), ("max_results", "3")],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["total"], 1);
    assert_eq!(body["channels"][0]["id"], "UC_example");
    assert_eq!(
        body["channels"][0]["url"],
        "https://www.youtube.com/channel/UC_example"
    );
    assert_eq!(body["channels"][0]["description"], "Descripción");
    assert_eq!(body["next_page_token"], "next+/=");
    assert_eq!(body["prev_page_token"], Value::Null);
    let (status, body) = search_request(
        app,
        "/channels/search",
        &[("q", "Jesús G. Maestro"), ("page_token", "next+/=")],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["channels"], json!([]));
    assert_eq!(body["total"], 0);
    assert_eq!(body["next_page_token"], Value::Null);
    assert_eq!(body["prev_page_token"], "previous");
    let requests = state.requests.lock().unwrap();
    assert_eq!(requests.len(), 2); // One upstream page per endpoint request.
    assert_eq!(requests[0].0, "search");
    assert_eq!(requests[0].1["q"], "Jesús G. Maestro");
    assert_eq!(requests[0].1["type"], "channel");
    assert_eq!(requests[0].1["part"], "snippet");
    assert_eq!(requests[0].1["maxResults"], "3");
    assert_eq!(requests[1].1["maxResults"], "25");
    assert_eq!(requests[1].1["pageToken"], "next+/=");
}

#[tokio::test]
async fn global_playlist_search_returns_playlist_metadata() {
    let mut state = MockState::new(HashMap::new());
    state.search_pages.insert(
        "".into(),
        (
            StatusCode::OK,
            json!({"items": [result("playlistId", "PL_example"), result("playlistId", "PL_empty"), result("playlistId", "PL_unavailable")], "nextPageToken": "more"}),
        ),
    );
    state.playlists_pages.insert(
        "".into(),
        (
            StatusCode::OK,
            json!({"items": [
        {"id": "PL_empty", "contentDetails": {"itemCount": 0}},
        {"id": "PL_example", "contentDetails": {"itemCount": 42}}
    ], "nextPageToken": "ignored-details-token"}),
        ),
    );
    let (app, _server) = setup(state.clone()).await;
    let (status, body) = search_request(
        app,
        "/playlists/search",
        &[("q", "teoría literaria"), ("max_results", "50")],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["channel_id"], Value::Null);
    assert_eq!(body["total"], 3);
    assert_eq!(body["playlists"][0]["id"], "PL_example");
    assert_eq!(
        body["playlists"][0]["url"],
        "https://www.youtube.com/playlist?list=PL_example"
    );
    assert_eq!(body["playlists"][0]["channel_id"], "UC_example");
    assert_eq!(body["playlists"][0]["channel_title"], "Jesús G. Maestro");
    assert_eq!(body["playlists"][0]["video_count"], 42);
    assert_eq!(body["playlists"][1]["id"], "PL_empty");
    assert_eq!(body["playlists"][1]["video_count"], 0);
    assert_eq!(body["playlists"][2]["id"], "PL_unavailable");
    assert_eq!(body["playlists"][2]["video_count"], Value::Null);
    assert_eq!(body["next_page_token"], "more");
    let requests = state.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].1["type"], "playlist");
    assert_eq!(requests[0].1["q"], "teoría literaria");
    assert_eq!(requests[0].1["maxResults"], "50");
    assert!(!requests[0].1.contains_key("channelId"));
    assert_eq!(requests[1].0, "playlists");
    assert_eq!(requests[1].1["part"], "contentDetails");
    assert_eq!(requests[1].1["id"], "PL_example,PL_empty,PL_unavailable");
    assert!(!requests[1].1.contains_key("pageToken"));
}

#[tokio::test]
async fn channel_playlist_listing_uses_playlists_api_and_pagination() {
    let mut state = MockState::new(HashMap::new());
    state.playlists_pages.insert("opaque+/=".into(), (StatusCode::OK, json!({"items": [{"id": "PL_example", "snippet": snippet(), "contentDetails": {"itemCount": 57}}], "prevPageToken": "previous"})));
    let (app, _server) = setup(state.clone()).await;
    let (status, body) = search_request(
        app,
        "/playlists/search",
        &[
            ("channel", "https://www.youtube.com/@Jes%C3%BAsGMaestro"),
            ("page_token", "opaque+/="),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["channel_id"], "UC_example");
    assert_eq!(body["playlists"][0]["id"], "PL_example");
    assert_eq!(body["playlists"][0]["video_count"], 57);
    assert_eq!(body["prev_page_token"], "previous");
    let requests = state.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].0, "channels");
    assert_eq!(requests[0].1["forHandle"], "@JesúsGMaestro");
    assert_eq!(requests[1].0, "playlists");
    assert_eq!(requests[1].1["part"], "snippet,contentDetails");
    assert_eq!(requests[1].1["channelId"], "UC_example");
    assert_eq!(requests[1].1["pageToken"], "opaque+/=");
    assert!(!requests[1].1.contains_key("q"));
}

#[tokio::test]
async fn playlist_search_can_combine_term_and_channel_id() {
    let mut state = MockState::new(HashMap::new());
    state
        .search_pages
        .insert("page2".into(), (StatusCode::OK, json!({"items": []})));
    let (app, _server) = setup(state.clone()).await;
    let id = "UC_x5XG1OV2P6uZZ5FSM9Ttw";
    let (status, body) = search_request(
        app,
        "/playlists/search",
        &[("channel", id), ("q", "Rust"), ("page_token", "page2")],
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["playlists"], json!([]));
    let requests = state.requests.lock().unwrap();
    assert_eq!(requests.len(), 2); // Empty searches do not need a counts lookup.
    assert_eq!(requests[0].1["id"], id);
    assert_eq!(requests[1].0, "search");
    assert_eq!(requests[1].1["channelId"], "UC_example");
    assert_eq!(requests[1].1["type"], "playlist");
    assert_eq!(requests[1].1["q"], "Rust");
    assert_eq!(requests[1].1["pageToken"], "page2");
}

#[tokio::test]
async fn invalid_search_parameters_fail_before_calling_upstream() {
    let state = MockState::new(HashMap::new());
    let (app, _server) = setup(state.clone()).await;
    for (path, params) in [
        ("/channels/search", vec![]),
        ("/channels/search", vec![("q", " ")]),
        (
            "/channels/search",
            vec![("q", "Rust"), ("max_results", "0")],
        ),
        (
            "/channels/search",
            vec![("q", "Rust"), ("max_results", "51")],
        ),
        (
            "/channels/search",
            vec![("q", "Rust"), ("max_results", "-1")],
        ),
        ("/channels/search", vec![("q", "Rust"), ("page_token", " ")]),
        ("/playlists/search", vec![]),
        ("/playlists/search", vec![("q", "")]),
        (
            "/playlists/search",
            vec![("channel", "@example"), ("q", " ")],
        ),
        (
            "/playlists/search",
            vec![("channel", "https://example.com/@example")],
        ),
        (
            "/playlists/search",
            vec![("q", "Rust"), ("max_results", "256")],
        ),
        ("/playlists/search", vec![("q", "Rust"), ("page_token", "")]),
    ] {
        let (status, body) = search_request(app.clone(), path, &params).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{path} {params:?}: {body}");
    }
    assert!(state.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn unknown_playlist_channel_returns_404() {
    let mut state = MockState::new(HashMap::new());
    state.channel_body = json!({"items": []});
    let (app, _server) = setup(state.clone()).await;
    let (status, _) = search_request(app, "/playlists/search", &[("channel", "@missing")]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(state.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn failed_or_malformed_search_returns_502_without_secrets() {
    for (path, params, status, body) in [
        (
            "/channels/search",
            vec![("q", "Rust")],
            StatusCode::FORBIDDEN,
            json!({"error": "test-secret-key"}),
        ),
        (
            "/channels/search",
            vec![("q", "Rust")],
            StatusCode::OK,
            json!({"items": [result("playlistId", "wrong-kind")]}),
        ),
        (
            "/playlists/search",
            vec![("q", "Rust")],
            StatusCode::OK,
            json!({"items": [result("channelId", "wrong-kind")]}),
        ),
        (
            "/playlists/search",
            vec![("q", "Rust")],
            StatusCode::OK,
            json!({"items": [{"id": {"playlistId": "PL_id"}}]}),
        ),
        (
            "/playlists/search",
            vec![("channel", "@example")],
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error": "test-secret-key"}),
        ),
    ] {
        let mut state = MockState::new(HashMap::new());
        state.search_pages.insert("".into(), (status, body.clone()));
        state.playlists_pages.insert("".into(), (status, body));
        let (app, _server) = setup(state).await;
        let (status, body) = search_request(app, path, &params).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body, json!({"error": "YouTube API request failed"}));
    }
}

#[tokio::test]
async fn failed_or_malformed_playlist_counts_fail_the_whole_search() {
    for (status, body) in [
        (StatusCode::FORBIDDEN, json!({"error": "test-secret-key"})),
        (
            StatusCode::OK,
            json!({"items": [{"id": "PL_example", "contentDetails": {}}]}),
        ),
    ] {
        let mut state = MockState::new(HashMap::new());
        state.search_pages.insert(
            "".into(),
            (
                StatusCode::OK,
                json!({"items": [result("playlistId", "PL_example")]}),
            ),
        );
        state.playlists_pages.insert("".into(), (status, body));
        let (app, _server) = setup(state.clone()).await;
        let (status, body) = search_request(
            app,
            "/playlists/search",
            &[("channel", "@example"), ("q", "literatura")],
        )
        .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert_eq!(body, json!({"error": "YouTube API request failed"}));
        assert_eq!(state.requests.lock().unwrap().len(), 3);
    }
}
