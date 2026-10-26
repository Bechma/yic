use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use percent_encoding::percent_decode_str;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::ApiError;

mod search;
pub(crate) use search::{
    ChannelSearchQuery, ChannelSearchResponse, PlaylistSearchQuery, PlaylistSearchResponse,
};

#[derive(Default, Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct VideosQuery {
    /// YouTube @handle or handle URL. Provide exactly one of channel and playlist_id.
    #[param(example = "@JesúsGMaestro", min_length = 2)]
    pub(crate) channel: Option<String>,
    /// Playlist ID from /playlists/search or a YouTube playlist URL's list parameter. Provide exactly one source.
    #[param(example = "PL_example", min_length = 1, pattern = "^[A-Za-z0-9_-]+$")]
    playlist_id: Option<String>,
    /// Filter estimated Shorts/non-Shorts. Both exclude unknown classifications.
    is_short: Option<bool>,
    /// Inclusive minimum length in seconds; must not exceed max_duration.
    #[param(minimum = 0)]
    min_duration: Option<u64>,
    /// Inclusive maximum length in seconds.
    #[param(minimum = 0)]
    max_duration: Option<u64>,
    /// Return at most this many matches after skip. Omit for all; 0 returns none.
    #[param(minimum = 0)]
    top: Option<usize>,
    /// Number of matching videos to skip before returning results.
    #[serde(default)]
    #[param(minimum = 0, default = 0)]
    skip: usize,
}

impl VideosQuery {
    pub(crate) fn validate(&self) -> Result<(), ApiError> {
        match (self.channel.as_deref(), self.playlist_id.as_deref()) {
            (Some(_), None) => {}
            (None, Some(id))
                if !id.trim().is_empty()
                    && id
                        .trim()
                        .bytes()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-')) => {}
            (None, Some(_)) => {
                return Err(ApiError::InvalidFilters(
                    "playlist_id must be a nonempty playlist ID, not a URL",
                ));
            }
            _ => {
                return Err(ApiError::InvalidFilters(
                    "provide exactly one of channel and playlist_id",
                ));
            }
        }
        if let (Some(min), Some(max)) = (self.min_duration, self.max_duration)
            && min > max
        {
            return Err(ApiError::InvalidFilters(
                "min_duration must not exceed max_duration",
            ));
        }
        Ok(())
    }

    fn matches(&self, video: &Video) -> bool {
        self.is_short
            .is_none_or(|value| video.is_short == Some(value))
            && self
                .min_duration
                .is_none_or(|min| video.duration_seconds.is_some_and(|value| value >= min))
            && self
                .max_duration
                .is_none_or(|max| video.duration_seconds.is_some_and(|value| value <= max))
    }
}

#[derive(Clone)]
pub struct YouTubeClient {
    client: Client,
    api_key: String,
    base_url: Url,
}

impl YouTubeClient {
    pub(crate) fn http_client(&self) -> Client {
        self.client.clone()
    }

    pub fn new(api_key: String) -> Result<Self, reqwest::Error> {
        Ok(Self {
            client: Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(30))
                .build()?,
            api_key,
            base_url: Url::parse("https://www.googleapis.com/youtube/v3/")
                .expect("valid YouTube API base URL"),
        })
    }

    #[tracing::instrument(name = "youtube.request", skip_all, fields(resource = resource))]
    async fn get<T: DeserializeOwned>(
        &self,
        resource: &str,
        query: &[(&str, &str)],
    ) -> Result<T, ApiError> {
        let url = self
            .base_url
            .join(resource)
            .map_err(|_| ApiError::Upstream)?;
        tracing::debug!("Sending YouTube Data API request");
        let response = self
            .client
            .get(url)
            .query(&[("key", self.api_key.as_str())])
            .query(query)
            .send()
            .await?;
        tracing::info!(
            upstream_status = response.status().as_u16(),
            "YouTube Data API responded"
        );
        Ok(response.error_for_status()?.json().await?)
    }

    #[tracing::instrument(name = "videos.list", skip_all)]
    pub(crate) async fn videos(&self, filters: &VideosQuery) -> Result<VideosResponse, ApiError> {
        let (channel_id, playlist_id) = if let Some(channel) = filters.channel.as_deref() {
            let channel = self.uploads_playlist(channel).await?;
            (
                channel.id,
                channel.content_details.related_playlists.uploads,
            )
        } else if let Some(id) = filters.playlist_id.as_deref() {
            let id = id.trim();
            let page: SourcePlaylistList = self
                .get("playlists", &[("part", "snippet"), ("id", id)])
                .await
                .map_err(playlist_error)?;
            let playlist = page
                .items
                .into_iter()
                .next()
                .ok_or(ApiError::PlaylistNotFound)?;
            (playlist.snippet.channel_id, playlist.id)
        } else {
            return Err(ApiError::InvalidFilters(
                "provide exactly one of channel and playlist_id",
            ));
        };
        self.playlist_videos(channel_id, playlist_id, filters).await
    }

    async fn uploads_playlist(&self, input: &str) -> Result<Channel, ApiError> {
        let handle = normalize_handle(input)?;
        let channels: ChannelList = self
            .get(
                "channels",
                &[("part", "contentDetails"), ("forHandle", &handle)],
            )
            .await?;
        let channel = channels
            .items
            .into_iter()
            .next()
            .ok_or(ApiError::NotFound)?;
        if channel.content_details.related_playlists.uploads.is_empty() {
            return Err(ApiError::Upstream);
        }
        Ok(channel)
    }

    async fn playlist_videos(
        &self,
        channel_id: String,
        playlist_id: String,
        filters: &VideosQuery,
    ) -> Result<VideosResponse, ApiError> {
        let mut videos = Vec::new();
        let mut skip = filters.skip;
        let mut page_token = None;
        let mut seen_tokens = HashSet::new();
        let mut pages = 0_u64;
        'pages: while filters.top.is_none_or(|top| videos.len() < top) {
            let mut query = vec![
                ("part", "snippet,contentDetails"),
                ("playlistId", playlist_id.as_str()),
                ("maxResults", "50"),
            ];
            if let Some(token) = page_token.as_deref() {
                query.push(("pageToken", token));
            }
            let page: PlaylistPage = self
                .get("playlistItems", &query)
                .await
                .map_err(playlist_error)?;
            pages += 1;
            tracing::info!(
                page = pages,
                items = page.items.len(),
                "Playlist page retrieved"
            );
            // Keep details requests within the API's 50-video batch size. Match
            // by ID because videos.list need not return the playlist's order.
            for batch in page.items.chunks(50) {
                let ids = batch
                    .iter()
                    .map(|item| item.content_details.video_id.as_str())
                    .collect::<Vec<_>>()
                    .join(",");
                let details: VideoDetailsList = self
                    .get(
                        "videos",
                        &[
                            ("part", "contentDetails,player"),
                            ("id", &ids),
                            ("maxHeight", "1080"),
                            ("maxWidth", "1920"),
                        ],
                    )
                    .await?;
                let details: HashMap<_, _> = details
                    .items
                    .into_iter()
                    .map(|video| (video.id.clone(), video))
                    .collect();
                for item in batch {
                    let id = &item.content_details.video_id;
                    let metadata = details.get(id);
                    let duration_seconds = metadata
                        .map(|metadata| parse_duration_seconds(&metadata.content_details.duration))
                        .transpose()?;
                    let published_at = item.content_details.video_published_at.clone();
                    let video = Video {
                        id: id.clone(),
                        url: format!("https://www.youtube.com/watch?v={id}"),
                        title: item.snippet.title.clone(),
                        description: item.snippet.description.clone(),
                        is_short: estimate_short(
                            duration_seconds,
                            metadata.and_then(|metadata| metadata.player.as_ref()),
                            published_at.as_deref(),
                        ),
                        duration: duration_seconds.map(format_duration),
                        duration_seconds,
                        published_at,
                    };
                    if !filters.matches(&video) {
                        continue;
                    }
                    if skip > 0 {
                        skip -= 1;
                        continue;
                    }
                    videos.push(video);
                    if filters.top.is_some_and(|top| videos.len() >= top) {
                        break 'pages;
                    }
                }
            }
            match page.next_page_token.filter(|token| !token.is_empty()) {
                Some(token) => {
                    // Fail rather than loop forever if upstream repeats a cursor.
                    if !seen_tokens.insert(token.clone()) {
                        return Err(ApiError::Upstream);
                    }
                    page_token = Some(token);
                }
                None => break,
            }
        }

        tracing::info!(pages, videos = videos.len(), "Video listing completed");
        Ok(VideosResponse {
            channel_id,
            playlist_id,
            total: videos.len(),
            shorts_detection: "estimated",
            videos,
        })
    }
}

fn playlist_error(error: ApiError) -> ApiError {
    match error {
        ApiError::UpstreamNotFound => ApiError::PlaylistNotFound,
        error => error,
    }
}

fn format_duration(seconds: u64) -> String {
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let seconds = seconds % 60;
    if hours == 0 {
        format!("{minutes:02}:{seconds:02}")
    } else {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    }
}

// YouTube documents integer ISO 8601 durations with days, hours, minutes,
// and seconds. Reject malformed/overflowing values rather than invent a length.
fn parse_duration_seconds(value: &str) -> Result<u64, ApiError> {
    let value = value.strip_prefix('P').ok_or(ApiError::Upstream)?;
    let mut total = 0_u64;
    let mut start = 0;
    let mut in_time = false;
    let mut last_rank = 0;
    let mut saw_time_component = false;
    for (index, c) in value.char_indices() {
        if c.is_ascii_digit() {
            continue;
        }
        if c == 'T' && !in_time && start == index {
            in_time = true;
            start = index + 1;
            continue;
        }
        let (rank, multiplier) = match (in_time, c) {
            (false, 'D') => (1, 86_400),
            (true, 'H') => (2, 3_600),
            (true, 'M') => (3, 60),
            (true, 'S') => (4, 1),
            _ => return Err(ApiError::Upstream),
        };
        if rank <= last_rank {
            return Err(ApiError::Upstream);
        }
        let amount = value[start..index]
            .parse::<u64>()
            .map_err(|_| ApiError::Upstream)?;
        total = amount
            .checked_mul(multiplier)
            .and_then(|amount| total.checked_add(amount))
            .ok_or(ApiError::Upstream)?;
        last_rank = rank;
        saw_time_component |= in_time;
        start = index + 1;
    }
    if last_rank == 0 || start != value.len() || (in_time && !saw_time_component) {
        return Err(ApiError::Upstream);
    }
    Ok(total)
}

fn estimate_short(
    duration: Option<u64>,
    player: Option<&Player>,
    published_at: Option<&str>,
) -> Option<bool> {
    let duration = duration?;
    if duration == 0 {
        return None; // Often an upcoming/live stream without a known length.
    }
    if duration > 180 {
        return Some(false);
    }
    let player = player?;
    // API 'long' values are decimal strings. Missing dimensions mean the
    // aspect ratio is unknown, even when embedHtml has default dimensions.
    let width = player
        .embed_width
        .as_deref()?
        .parse::<u64>()
        .ok()
        .filter(|&v| v > 0)?;
    let height = player
        .embed_height
        .as_deref()?
        .parse::<u64>()
        .ok()
        .filter(|&v| v > 0)?;
    if width > height {
        return Some(false);
    }
    if duration <= 60 {
        return Some(true);
    }
    let date = chrono::DateTime::parse_from_rfc3339(published_at?)
        .ok()?
        .to_utc()
        .date_naive();
    let standard_cutoff = chrono::NaiveDate::from_ymd_opt(2024, 10, 15)?;
    let artist_cutoff = chrono::NaiveDate::from_ymd_opt(2025, 12, 8)?;
    if date < standard_cutoff {
        Some(false)
    } else if date >= artist_cutoff {
        Some(true)
    } else {
        // Different rollout dates for standard and Official Artist Channels;
        // the public API does not identify which rule applies to this channel.
        None
    }
}

fn normalize_handle(input: &str) -> Result<String, ApiError> {
    let input = input.trim();
    let path = if input.starts_with('@') {
        input.to_owned()
    } else {
        let url = Url::parse(input).map_err(|_| ApiError::BadRequest)?;
        if !matches!(url.scheme(), "http" | "https")
            || !matches!(
                url.host_str(),
                Some("youtube.com" | "www.youtube.com" | "m.youtube.com")
            )
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some()
        {
            return Err(ApiError::BadRequest);
        }
        url.path()
            .trim_start_matches('/')
            .trim_end_matches('/')
            .to_owned()
    };
    let decoded = percent_decode_str(&path)
        .decode_utf8()
        .map_err(|_| ApiError::BadRequest)?;
    let handle = decoded.strip_prefix('@').ok_or(ApiError::BadRequest)?;
    if handle.is_empty()
        || handle.chars().any(|c| {
            c.is_whitespace()
                || c.is_control()
                || (c.is_ascii() && !c.is_ascii_alphanumeric() && !matches!(c, '_' | '-' | '.'))
        })
    {
        return Err(ApiError::BadRequest);
    }
    Ok(decoded.into_owned())
}

#[derive(Serialize, utoipa::ToSchema)]
pub(crate) struct VideosResponse {
    /// Channel whose uploads were requested, or the owner of the requested playlist (not necessarily each video's creator).
    channel_id: String,
    /// The channel's uploads playlist ID or the explicitly requested playlist ID.
    playlist_id: String,
    /// Number returned after applying filters, skip, and top.
    total: usize,
    /// All is_short classifications are estimates, not confirmed by YouTube.
    #[schema(example = "estimated")]
    shorts_detection: &'static str,
    videos: Vec<Video>,
}

#[derive(Serialize, utoipa::ToSchema)]
struct Video {
    id: String,
    title: String,
    description: String,
    url: String,
    /// Video publication time, if available.
    #[schema(format = DateTime)]
    published_at: Option<String>,
    /// Length in seconds, or null if video metadata is unavailable.
    #[schema(example = 3723, minimum = 0)]
    duration_seconds: Option<u64>,
    /// mm:ss below one hour, otherwise hh:mm:ss; null when duration is unknown.
    #[schema(example = "01:02:03")]
    duration: Option<String>,
    /// Estimated Shorts status; null when metadata or rollout rules are inconclusive.
    is_short: Option<bool>,
}

#[derive(Deserialize)]
struct VideoDetailsList {
    #[serde(default)]
    items: Vec<VideoDetails>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VideoDetails {
    id: String,
    content_details: DurationDetails,
    player: Option<Player>,
}

#[derive(Deserialize)]
struct DurationDetails {
    duration: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Player {
    embed_width: Option<String>,
    embed_height: Option<String>,
}

#[derive(Deserialize)]
struct ChannelList {
    #[serde(default)]
    items: Vec<Channel>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Channel {
    id: String,
    content_details: ChannelContentDetails,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChannelContentDetails {
    related_playlists: RelatedPlaylists,
}

#[derive(Deserialize)]
struct RelatedPlaylists {
    uploads: String,
}

#[derive(Deserialize)]
struct SourcePlaylistList {
    #[serde(default)]
    items: Vec<SourcePlaylist>,
}

#[derive(Deserialize)]
struct SourcePlaylist {
    id: String,
    snippet: SourcePlaylistSnippet,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SourcePlaylistSnippet {
    channel_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaylistPage {
    #[serde(default)]
    items: Vec<PlaylistItem>,
    next_page_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaylistItem {
    snippet: VideoSnippet,
    content_details: VideoContentDetails,
}

#[derive(Deserialize)]
struct VideoSnippet {
    title: String,
    description: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VideoContentDetails {
    video_id: String,
    video_published_at: Option<String>,
}

#[cfg(test)]
mod tests;
