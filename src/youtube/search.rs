use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{YouTubeClient, normalize_handle};
use crate::ApiError;

#[derive(Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct ChannelSearchQuery {
    /// Nonempty channel name or search term.
    #[param(min_length = 1, example = "Jesús G. Maestro")]
    q: String,
    /// Number of results per upstream page.
    #[param(minimum = 1, maximum = 50, default = 25)]
    max_results: Option<u8>,
    /// Opaque next_page_token/prev_page_token from a previous search. Keep other parameters the same.
    #[param(min_length = 1)]
    page_token: Option<String>,
}

#[derive(Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Query)]
pub(crate) struct PlaylistSearchQuery {
    /// Channel @handle, handle URL, or canonical UC… channel ID. Required when q is omitted.
    #[param(min_length = 2, example = "@JesúsGMaestro")]
    channel: Option<String>,
    /// Nonempty playlist search term. Omit to list a channel's playlists; omit channel for global search.
    #[param(min_length = 1, example = "literatura")]
    q: Option<String>,
    /// Number of results per upstream page.
    #[param(minimum = 1, maximum = 50, default = 25)]
    max_results: Option<u8>,
    /// Opaque next_page_token/prev_page_token from a previous request with the same search parameters.
    #[param(min_length = 1)]
    page_token: Option<String>,
}

fn validate_pagination(max_results: Option<u8>, page_token: Option<&str>) -> Result<u8, ApiError> {
    let max_results = max_results.unwrap_or(25);
    if !(1..=50).contains(&max_results) {
        return Err(ApiError::InvalidFilters("max_results must be from 1 to 50"));
    }
    if page_token.is_some_and(|token| token.trim().is_empty()) {
        return Err(ApiError::InvalidFilters("page_token must not be empty"));
    }
    Ok(max_results)
}

fn search_term(q: &str) -> Result<&str, ApiError> {
    let q = q.trim();
    if q.is_empty() {
        return Err(ApiError::InvalidFilters("q must not be empty"));
    }
    Ok(q)
}

impl YouTubeClient {
    #[tracing::instrument(name = "channels.search", skip_all)]
    pub(crate) async fn search_channels(
        &self,
        query: &ChannelSearchQuery,
    ) -> Result<ChannelSearchResponse, ApiError> {
        let q = search_term(&query.q)?;
        let max_results =
            validate_pagination(query.max_results, query.page_token.as_deref())?.to_string();
        let mut params = vec![
            ("part", "snippet"),
            ("type", "channel"),
            ("q", q),
            ("maxResults", &max_results),
        ];
        if let Some(token) = query.page_token.as_deref() {
            params.push(("pageToken", token));
        }
        let page: Page<SearchItem> = self.get("search", &params).await?;
        let channels = page
            .items
            .into_iter()
            .map(|item| {
                let id = item.id.channel_id.ok_or(ApiError::Upstream)?;
                Ok(ChannelSummary {
                    url: format!("https://www.youtube.com/channel/{id}"),
                    id,
                    title: item.snippet.title,
                    description: item.snippet.description,
                    published_at: item.snippet.published_at,
                })
            })
            .collect::<Result<Vec<_>, ApiError>>()?;
        Ok(ChannelSearchResponse {
            total: channels.len(),
            channels,
            next_page_token: page.next_page_token,
            prev_page_token: page.prev_page_token,
        })
    }

    #[tracing::instrument(name = "playlists.search", skip_all)]
    pub(crate) async fn search_playlists(
        &self,
        query: &PlaylistSearchQuery,
    ) -> Result<PlaylistSearchResponse, ApiError> {
        let q = query.q.as_deref().map(search_term).transpose()?;
        let max_results =
            validate_pagination(query.max_results, query.page_token.as_deref())?.to_string();
        if query.channel.is_none() && q.is_none() {
            return Err(ApiError::InvalidFilters("provide channel and/or q"));
        }
        let channel_id = match query.channel.as_deref() {
            Some(channel) => Some(self.resolve_channel_id(channel).await?),
            None => None,
        };
        let part = if q.is_some() {
            "snippet"
        } else {
            "snippet,contentDetails"
        };
        let mut params = vec![("part", part), ("maxResults", &max_results)];
        if let Some(id) = channel_id.as_deref() {
            params.push(("channelId", id));
        }
        if let Some(token) = query.page_token.as_deref() {
            params.push(("pageToken", token));
        }
        let (playlists, next_page_token, prev_page_token) = if let Some(q) = q {
            // search.list supports both global search and channelId + q.
            params.extend([("type", "playlist"), ("q", q)]);
            let page: Page<SearchItem> = self.get("search", &params).await?;
            let mut playlists = page
                .items
                .into_iter()
                .map(|item| {
                    let id = item.id.playlist_id.ok_or(ApiError::Upstream)?;
                    Ok(PlaylistSummary::new(id, item.snippet, None))
                })
                .collect::<Result<Vec<_>, ApiError>>()?;
            self.add_playlist_counts(&mut playlists).await?;
            (playlists, page.next_page_token, page.prev_page_token)
        } else {
            // playlists.list enumerates a channel's public playlists without
            // spending a search query or requiring a text query.
            let page: Page<PlaylistItem> = self.get("playlists", &params).await?;
            let playlists = page
                .items
                .into_iter()
                .map(|item| {
                    PlaylistSummary::new(
                        item.id,
                        item.snippet,
                        Some(item.content_details.item_count),
                    )
                })
                .collect();
            (playlists, page.next_page_token, page.prev_page_token)
        };
        Ok(PlaylistSearchResponse {
            channel_id,
            total: playlists.len(),
            playlists,
            next_page_token,
            prev_page_token,
        })
    }

    async fn add_playlist_counts(&self, playlists: &mut [PlaylistSummary]) -> Result<(), ApiError> {
        // Search resources omit counts; enrich by ID without changing their
        // relevance order or using the detail lookup's pagination metadata.
        for batch in playlists.chunks_mut(50) {
            let ids = batch
                .iter()
                .map(|playlist| playlist.id.as_str())
                .collect::<Vec<_>>()
                .join(",");
            let page: Page<PlaylistCount> = self
                .get("playlists", &[("part", "contentDetails"), ("id", &ids)])
                .await?;
            let counts: HashMap<_, _> = page
                .items
                .into_iter()
                .map(|item| (item.id, item.content_details.item_count))
                .collect();
            for playlist in batch {
                // A playlist can disappear or become private between calls.
                // Preserve its search result and report an unknown count.
                playlist.video_count = counts.get(&playlist.id).copied();
            }
        }
        Ok(())
    }

    async fn resolve_channel_id(&self, input: &str) -> Result<String, ApiError> {
        let input = input.trim();
        let is_id = input.starts_with("UC")
            && input.len() == 24
            && input
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'));
        let (filter, value) = if is_id {
            ("id", input.to_owned())
        } else {
            ("forHandle", normalize_handle(input)?)
        };
        let page: Page<ChannelId> = self
            .get("channels", &[("part", "id"), (filter, &value)])
            .await?;
        page.items
            .into_iter()
            .next()
            .map(|channel| channel.id)
            .ok_or(ApiError::NotFound)
    }
}

#[derive(Serialize, utoipa::ToSchema)]
pub(crate) struct ChannelSearchResponse {
    /// Number of channels returned in this page.
    total: usize,
    channels: Vec<ChannelSummary>,
    /// Cursor for the next page; null when absent.
    next_page_token: Option<String>,
    /// Cursor for the previous page; null when absent.
    prev_page_token: Option<String>,
}

#[derive(Serialize, utoipa::ToSchema)]
struct ChannelSummary {
    id: String,
    title: String,
    description: String,
    url: String,
    #[schema(format = DateTime)]
    published_at: Option<String>,
}

#[derive(Serialize, utoipa::ToSchema)]
pub(crate) struct PlaylistSearchResponse {
    /// Resolved channel when scoped; null for a global search.
    channel_id: Option<String>,
    /// Number of playlists returned in this page.
    total: usize,
    playlists: Vec<PlaylistSummary>,
    /// Cursor for the next page; null when absent.
    next_page_token: Option<String>,
    /// Cursor for the previous page; null when absent.
    prev_page_token: Option<String>,
}

#[derive(Serialize, utoipa::ToSchema)]
struct PlaylistSummary {
    id: String,
    title: String,
    description: String,
    url: String,
    channel_id: String,
    channel_title: String,
    /// YouTube's reported number of videos; null if the playlist is unavailable during the count lookup.
    #[schema(minimum = 0, example = 42)]
    video_count: Option<u64>,
    #[schema(format = DateTime)]
    published_at: Option<String>,
}

impl PlaylistSummary {
    fn new(id: String, snippet: Snippet, video_count: Option<u64>) -> Self {
        Self {
            url: format!("https://www.youtube.com/playlist?list={id}"),
            id,
            title: snippet.title,
            description: snippet.description,
            channel_id: snippet.channel_id,
            channel_title: snippet.channel_title,
            video_count,
            published_at: snippet.published_at,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", bound(deserialize = "T: Deserialize<'de>"))]
struct Page<T> {
    #[serde(default)]
    items: Vec<T>,
    next_page_token: Option<String>,
    prev_page_token: Option<String>,
}

#[derive(Deserialize)]
struct ChannelId {
    id: String,
}

#[derive(Deserialize)]
struct SearchItem {
    id: SearchId,
    snippet: Snippet,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SearchId {
    channel_id: Option<String>,
    playlist_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaylistItem {
    id: String,
    snippet: Snippet,
    content_details: PlaylistContentDetails,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaylistCount {
    id: String,
    content_details: PlaylistContentDetails,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlaylistContentDetails {
    item_count: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Snippet {
    title: String,
    description: String,
    channel_id: String,
    channel_title: String,
    published_at: Option<String>,
}
