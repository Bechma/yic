use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tower::ServiceExt;
use yic::{YouTubeClient, app};

async fn get(router: Router, path: &str) -> (StatusCode, String) {
    let response = router
        .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

fn router() -> Router {
    app(YouTubeClient::new("not-a-real-api-key".into()).unwrap())
}

#[tokio::test]
async fn generated_openapi_documents_routes_parameters_and_response_schemas() {
    let (status, body) = get(router(), "/api-docs/openapi.json").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body.contains("not-a-real-api-key"));
    let spec: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(spec["openapi"], "3.1.0");
    assert_eq!(spec["info"]["title"], "YouTube Channel API");
    assert_eq!(spec["paths"].as_object().unwrap().len(), 5);
    let download = &spec["paths"]["/videos/{video_id}/download"]["post"];
    for status in ["200", "206", "400", "404", "416", "502", "503", "504"] {
        assert!(
            download["responses"][status].is_object(),
            "download {status}"
        );
    }
    assert_eq!(
        download["requestBody"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/DownloadSelection"
    );
    assert!(download["responses"]["200"]["headers"]["Content-Disposition"].is_object());
    for status in ["200", "206"] {
        assert!(download["responses"][status]["headers"]["X-Download-Source-Client"].is_object());
    }
    let streams = &spec["paths"]["/videos/{video_id}/streams"]["get"];
    for status in ["200", "400", "502", "503", "504"] {
        assert!(streams["responses"][status].is_object());
    }
    assert_eq!(streams["parameters"][0]["in"], "path");
    assert_eq!(streams["parameters"][0]["required"], true);
    assert_eq!(
        streams["parameters"][0]["schema"]["pattern"],
        "^[A-Za-z0-9_-]{11}$"
    );
    for path in ["/videos", "/channels/search", "/playlists/search"] {
        let operation = &spec["paths"][path]["get"];
        for status in ["200", "400", "502", "504"] {
            assert!(
                operation["responses"][status].is_object(),
                "{path} {status}"
            );
        }
        assert_eq!(
            operation["responses"]["400"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ErrorBody"
        );
        if path != "/channels/search" {
            assert!(operation["responses"]["404"].is_object());
        }
        for parameter in operation["parameters"].as_array().unwrap() {
            assert_eq!(parameter["in"], "query");
        }
    }
    let videos = &spec["paths"]["/videos"]["get"]["parameters"];
    let param = |name: &str| {
        videos
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["name"] == name)
            .unwrap()
    };
    assert_eq!(param("channel")["required"], false);
    assert_eq!(param("playlist_id")["required"], false);
    assert_eq!(
        param("playlist_id")["schema"]["pattern"],
        "^[A-Za-z0-9_-]+$"
    );
    assert_eq!(param("skip")["required"], false);
    assert_eq!(param("skip")["schema"]["default"], 0);
    assert_eq!(param("min_duration")["schema"]["minimum"], 0);
    assert_eq!(param("is_short")["required"], false);
    assert_eq!(videos.as_array().unwrap().len(), 7);
    for path in ["/channels/search", "/playlists/search"] {
        let params = spec["paths"][path]["get"]["parameters"].as_array().unwrap();
        let max_results = params.iter().find(|p| p["name"] == "max_results").unwrap();
        assert_eq!(max_results["required"], false);
        assert_eq!(max_results["schema"]["minimum"], 1);
        assert_eq!(max_results["schema"]["maximum"], 50);
        assert_eq!(max_results["schema"]["default"], 25);
        assert!(params.iter().any(|p| p["name"] == "page_token"));
    }
    let schemas = &spec["components"]["schemas"];
    assert!(schemas["ErrorBody"]["properties"]["upstream_status"].is_object());
    assert!(
        !schemas["ErrorBody"]["required"]
            .as_array()
            .unwrap()
            .contains(&json!("upstream_status"))
    );
    for name in [
        "Video",
        "VideosResponse",
        "ChannelSummary",
        "ChannelSearchResponse",
        "PlaylistSummary",
        "PlaylistSearchResponse",
        "ErrorBody",
        "StreamsResponse",
        "StreamFormat",
        "Manifest",
        "ManifestKind",
        "SubtitleTrack",
        "SubtitleFormat",
        "SubtitleKind",
        "DownloadSelection",
    ] {
        assert!(schemas[name].is_object(), "missing {name}: {schemas}");
    }
    assert_eq!(
        schemas["Video"]["properties"]["duration"]["type"],
        json!(["string", "null"])
    );
    assert_eq!(
        schemas["Video"]["properties"]["is_short"]["type"],
        json!(["boolean", "null"])
    );
    assert!(schemas["Video"]["properties"]["duration_seconds"].is_object());
    for field in [
        "video_streams",
        "audio_streams",
        "subtitles",
        "manifests",
        "warnings",
        "coverage",
    ] {
        assert!(schemas["StreamsResponse"]["properties"][field].is_object());
    }
    assert_eq!(
        schemas["VideosResponse"]["properties"]["playlist_id"]["type"],
        "string"
    );
    assert_eq!(
        schemas["PlaylistSummary"]["properties"]["video_count"]["type"],
        json!(["integer", "null"])
    );
    assert_eq!(
        schemas["PlaylistSummary"]["properties"]["video_count"]["minimum"],
        0
    );
    for name in ["ChannelSearchResponse", "PlaylistSearchResponse"] {
        assert!(schemas[name]["properties"]["next_page_token"].is_object());
        assert!(schemas[name]["properties"]["prev_page_token"].is_object());
    }
}

#[tokio::test]
async fn swagger_ui_and_its_configuration_are_served_locally() {
    let router = router();
    let (status, html) = get(router.clone(), "/swagger-ui/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("swagger-ui-bundle.js"));
    let (status, javascript) = get(router.clone(), "/swagger-ui/swagger-initializer.js").await;
    assert_eq!(status, StatusCode::OK);
    assert!(javascript.contains("/api-docs/openapi.json"));
    let (status, bundle) = get(router, "/swagger-ui/swagger-ui-bundle.js").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!bundle.is_empty());
}
