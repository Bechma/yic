mod downloads;
mod mweb;
mod observability;
mod streams;
mod youtube;

pub use observability::init_tracing;
pub use streams::StreamExtractor;
pub use youtube::YouTubeClient;

use axum::{
    Json, Router,
    extract::{
        Path, Query, State,
        rejection::{JsonRejection, PathRejection, QueryRejection},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use downloads::{DownloadSelection, DownloadService};
use serde::Serialize;
use streams::StreamsResponse;
use utoipa_axum::{router::OpenApiRouter, routes};
use utoipa_swagger_ui::SwaggerUi;
use youtube::{
    ChannelSearchQuery, ChannelSearchResponse, PlaylistSearchQuery, PlaylistSearchResponse,
    VideosQuery, VideosResponse,
};

pub fn app(youtube: YouTubeClient) -> Router {
    let extractor = StreamExtractor::new(youtube.http_client());
    app_with_stream_extractor(youtube, extractor)
}

pub fn app_with_stream_extractor(youtube: YouTubeClient, extractor: StreamExtractor) -> Router {
    let downloads = DownloadService::new(extractor.clone());
    let (router, mut openapi) = OpenApiRouter::new()
        .routes(routes!(videos))
        .routes(routes!(search_channels))
        .routes(routes!(search_playlists))
        .with_state(youtube)
        .merge(
            OpenApiRouter::new()
                .routes(routes!(video_streams))
                .with_state(extractor),
        )
        .merge(
            OpenApiRouter::new()
                .routes(routes!(download_video))
                .with_state(downloads),
        )
        .split_for_parts();
    openapi.info.title = "YouTube Channel API".into();
    openapi.info.description = Some(
        "Retrieve videos from channels or playlists, search channels/playlists, and extract/download playback streams with Rust and a embedded deno_core worker pool. MWEB supports video-bound PO tokens and player-JavaScript transforms. API credentials are configured on the server; playback URLs are temporary and Shorts classifications are estimates."
            .into(),
    );
    router
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", openapi))
        .layer(axum::middleware::from_fn(observability::trace_request))
}

/// Download a selected video, audio, or subtitle format as an attachment.
///
/// Copy the format's download object from GET /videos/{video_id}/streams into the
/// JSON request body. Refreshes extraction to resolve a fresh URL and streams
/// bytes directly to the caller without saving or buffering the whole file.
/// A video-only format stays silent; muxed formats retain their audio. No muxing,
/// transcoding, or HLS/DASH segment assembly is performed. Only one byte Range
/// is supported. MWEB media is fetched in verified 1 MiB ranges, with up to eight
/// chunks in flight and ordered delivery. Full downloads return 200, requested
/// ranges return 206. Native clients retain upstream range behavior.
/// An iOS/TV media 403 (or 416 for a range known to fit the file) triggers one
/// MWEB attempt with the same format/type/track,
/// matching MIME type and any known file size. X-Download-Source-Client identifies
/// the actual client used. A missing equivalent retains the upstream 403 error.
/// Failures after response headers abort the transfer. Four transfers may run
/// at once, with a 30-second upstream read-idle timeout and no total file timeout.
#[utoipa::path(
    post,
    path = "/videos/{video_id}/download",
    tag = "Videos",
    params(
        ("video_id" = String, Path, description = "An 11-character YouTube video ID", min_length = 11, max_length = 11, pattern = "^[A-Za-z0-9_-]{11}$"),
        ("Range" = Option<String>, Header, description = "Optional single byte range", example = "bytes=0-1023")
    ),
    request_body = DownloadSelection,
    responses(
        (status = 200, description = "Selected file streamed as an attachment", content_type = "application/octet-stream", body = String, headers(("Content-Disposition" = String, description = "Attachment with generated filename"), ("Content-Length" = u64, description = "Length when provided upstream"), ("X-Download-Source-Client" = String, description = "Actual source client; MWEB after a native media rejection fallback"))),
        (status = 206, description = "Requested byte range as an attachment", content_type = "application/octet-stream", body = String, headers(("Content-Range" = String, description = "Range returned upstream"), ("X-Download-Source-Client" = String, description = "Actual source client"))),
        (status = 400, description = "Invalid ID, JSON selection, or byte range", body = ErrorBody),
        (status = 404, description = "Selected format is no longer available; refresh the streams response", body = ErrorBody),
        (status = 416, description = "Byte range unsatisfiable", headers(("Content-Range" = String, description = "Upstream resource length when provided"))),
        (status = 502, description = "Extraction or download failed, including expired or rejected media URLs", body = ErrorBody),
        (status = 503, description = "Download or extraction concurrency limit reached", body = ErrorBody),
        (status = 504, description = "Extraction or upstream download timed out before response started", body = ErrorBody)
    )
)]
async fn download_video(
    State(downloads): State<DownloadService>,
    path: Result<Path<String>, PathRejection>,
    headers: HeaderMap,
    selection: Result<Json<DownloadSelection>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Path(video_id) = path.map_err(|_| ApiError::InvalidFilters("invalid video_id"))?;
    let Json(selection) = selection.map_err(|_| {
        ApiError::InvalidFilters(
            "provide a format's download object as an application/json request body",
        )
    })?;
    downloads.download(&video_id, selection, headers).await
}

/// Get available video, audio, and subtitle streams for a video ID.
///
/// Uses native Rust HTTP requests to YouTube's internal player API and a
/// embedded deno_core worker pool for JavaScript, without yt-dlp. Combines available iOS/TV/MWEB formats, captions/translations,
/// and HLS/DASH manifests. No media is downloaded. URLs expire and can depend on
/// the server's IP and returned headers. MWEB uses embedded V8 isolates for BotGuard PO
/// tokens and player signature/n decoding. DRM, authentication and SABR decoding
/// remain unsupported. Failed clients and skipped formats produce warnings.
/// Player scripts are analyzed in embedded V8 and cached by revision after
/// parsing. At most two embedded V8 workers run alongside the four extraction slots.
#[utoipa::path(
    get,
    path = "/videos/{video_id}/streams",
    tag = "Videos",
    params(("video_id" = String, Path, description = "An 11-character YouTube video ID", example = "dQw4w9WgXcQ", min_length = 11, max_length = 11, pattern = "^[A-Za-z0-9_-]{11}$")),
    responses(
        (status = 200, description = "Available playback formats and subtitle tracks", body = StreamsResponse),
        (status = 400, description = "Invalid video ID", body = ErrorBody),
        (status = 502, description = "Extraction failed, video inaccessible, or invalid/oversized extractor output", body = ErrorBody),
        (status = 503, description = "All four extraction slots are in use", body = ErrorBody),
        (status = 504, description = "Extraction exceeded 120 seconds", body = ErrorBody)
    )
)]
async fn video_streams(
    State(extractor): State<StreamExtractor>,
    path: Result<Path<String>, PathRejection>,
) -> Result<Json<StreamsResponse>, ApiError> {
    let Path(video_id) = path.map_err(|_| ApiError::InvalidFilters("invalid video_id"))?;
    extractor.extract(&video_id).await.map(Json)
}

/// Search public YouTube channels.
///
/// Returns one upstream page in relevance order. Repeat the same search with
/// `page_token` to continue; `total` counts only this page's results.
#[utoipa::path(
    get,
    path = "/channels/search",
    tag = "Channels",
    params(ChannelSearchQuery),
    responses(
        (status = 200, description = "A page of matching channels", body = ChannelSearchResponse),
        (status = 400, description = "Missing/empty q or invalid pagination parameters", body = ErrorBody),
        (status = 502, description = "YouTube API request failed, including key/quota errors", body = ErrorBody),
        (status = 504, description = "YouTube API request timed out", body = ErrorBody)
    )
)]
async fn search_channels(
    State(youtube): State<YouTubeClient>,
    query: Result<Query<ChannelSearchQuery>, QueryRejection>,
) -> Result<Json<ChannelSearchResponse>, ApiError> {
    let Query(query) = query.map_err(|_| {
        ApiError::InvalidFilters("q is required; max_results must be an integer from 1 to 50")
    })?;
    youtube.search_channels(&query).await.map(Json)
}

/// List or search public playlists.
///
/// Provide `channel`, `q`, or both. A channel alone lists its public playlists;
/// a term alone searches globally; both search within that channel. Returns
/// one upstream page. Repeat the same parameters with `page_token` to continue.
/// Each playlist includes YouTube's video_count. Text searches use a batched
/// detail lookup; if a playlist becomes unavailable, its count is null.
#[utoipa::path(
    get,
    path = "/playlists/search",
    tag = "Playlists",
    params(PlaylistSearchQuery),
    responses(
        (status = 200, description = "A page of playlists; total counts this page's results", body = PlaylistSearchResponse),
        (status = 400, description = "Provide channel and/or q; supplied terms must be nonempty and max_results must be 1–50", body = ErrorBody),
        (status = 404, description = "The specified channel was not found", body = ErrorBody),
        (status = 502, description = "YouTube API request failed, including key/quota errors", body = ErrorBody),
        (status = 504, description = "YouTube API request timed out", body = ErrorBody)
    )
)]
async fn search_playlists(
    State(youtube): State<YouTubeClient>,
    query: Result<Query<PlaylistSearchQuery>, QueryRejection>,
) -> Result<Json<PlaylistSearchResponse>, ApiError> {
    let Query(query) = query.map_err(|_| {
        ApiError::InvalidFilters(
            "provide channel and/or q; max_results must be an integer from 1 to 50",
        )
    })?;
    youtube.search_playlists(&query).await.map(Json)
}

/// Retrieve videos from a channel's uploads or a specific playlist.
///
/// Provide exactly one of channel and playlist_id. Filters combine with AND;
/// skip and top apply after filtering in playlist
/// order. min_duration must not exceed max_duration. The server follows upstream
/// pages until top results are collected or all uploads have been visited. total
/// counts returned videos, not all potential matches. Shorts classification is an
/// estimate based on duration, player aspect ratio, and publication date; unknown
/// values are null and match neither is_short=true nor is_short=false. Failures
/// on any required upstream page fail the whole request rather than return a partial list.
#[utoipa::path(
    get,
    path = "/videos",
    tag = "Videos",
    params(VideosQuery),
    responses(
        (status = 200, description = "Matching playlist videos with durations and estimated Shorts classification", body = VideosResponse),
        (status = 400, description = "Provide exactly one of channel and playlist_id; min_duration must not exceed max_duration", body = ErrorBody),
        (status = 404, description = "The specified channel or playlist was not found", body = ErrorBody),
        (status = 502, description = "YouTube API request failed, including key/quota errors", body = ErrorBody),
        (status = 504, description = "YouTube API request timed out", body = ErrorBody)
    )
)]
async fn videos(
    State(youtube): State<YouTubeClient>,
    query: Result<Query<VideosQuery>, QueryRejection>,
) -> Result<Json<VideosResponse>, ApiError> {
    let Query(query) = query.map_err(|_| {
        ApiError::InvalidFilters("provide exactly one of channel and playlist_id; is_short must be true/false and duration/top/skip must be non-negative integers")
    })?;
    query.validate()?;
    youtube.videos(&query).await.map(Json)
}

#[derive(Debug)]
enum ApiError {
    BadRequest,
    InvalidFilters(&'static str),
    NotFound,
    PlaylistNotFound,
    Upstream,
    UpstreamNotFound,
    Timeout,
    StreamBusy,
    StreamExtraction,
    StreamTimeout,
    MwebExtraction,
    MwebAttestation,
    DownloadNotFound,
    DownloadBusy,
    DownloadUpstream,
    DownloadRejected(StatusCode),
    DownloadTimeout,
}

#[derive(Serialize, utoipa::ToSchema)]
struct ErrorBody {
    /// Public error message. Upstream bodies and API keys are never included.
    #[schema(example = "YouTube API request failed")]
    error: &'static str,
    /// HTTP status returned by YouTube's media server, when a download was rejected.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(example = 403, minimum = 100, maximum = 599)]
    upstream_status: Option<u16>,
}

impl From<reqwest::Error> for ApiError {
    fn from(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            Self::Timeout
        } else if error.status() == Some(StatusCode::NOT_FOUND) {
            Self::UpstreamNotFound
        } else {
            Self::Upstream
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let upstream_status = match &self {
            Self::DownloadRejected(status) => Some(status.as_u16()),
            _ => None,
        };
        let (status, message) = match self {
            Self::BadRequest => (
                StatusCode::BAD_REQUEST,
                "channel must be an @handle or a YouTube URL such as https://www.youtube.com/@handle",
            ),
            Self::InvalidFilters(message) => (StatusCode::BAD_REQUEST, message),
            Self::NotFound => (StatusCode::NOT_FOUND, "YouTube channel not found"),
            Self::PlaylistNotFound => (StatusCode::NOT_FOUND, "YouTube playlist not found"),
            Self::Upstream | Self::UpstreamNotFound => {
                (StatusCode::BAD_GATEWAY, "YouTube API request failed")
            }
            Self::Timeout => (StatusCode::GATEWAY_TIMEOUT, "YouTube API request timed out"),
            Self::StreamBusy => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Stream extractor is busy; try again later",
            ),
            Self::StreamExtraction => (
                StatusCode::BAD_GATEWAY,
                "Unable to extract streams for this video",
            ),
            Self::StreamTimeout => (StatusCode::GATEWAY_TIMEOUT, "Stream extraction timed out"),
            Self::MwebExtraction => (
                StatusCode::BAD_GATEWAY,
                "Unable to extract MWEB streams with the embedded V8 worker pool",
            ),
            Self::MwebAttestation => (
                StatusCode::BAD_GATEWAY,
                "YouTube did not issue a valid MWEB proof-of-origin token",
            ),
            Self::DownloadNotFound => (
                StatusCode::NOT_FOUND,
                "Selected stream not found; refresh the streams response",
            ),
            Self::DownloadBusy => (
                StatusCode::SERVICE_UNAVAILABLE,
                "All download slots are in use; try again later",
            ),
            Self::DownloadUpstream => (
                StatusCode::BAD_GATEWAY,
                "Unable to download the selected stream",
            ),
            Self::DownloadRejected(_) => (
                StatusCode::BAD_GATEWAY,
                "YouTube media server rejected the download",
            ),
            Self::DownloadTimeout => (StatusCode::GATEWAY_TIMEOUT, "Stream download timed out"),
        };
        // Never expose reqwest errors or Google's body: they may contain the API key.
        tracing::warn!(
            status = status.as_u16(),
            upstream_status,
            error = message,
            "Request failed"
        );
        (
            status,
            Json(ErrorBody {
                error: message,
                upstream_status,
            }),
        )
            .into_response()
    }
}
