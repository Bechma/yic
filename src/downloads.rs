use crate::{ApiError, StreamExtractor};
use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
};
use futures_util::{StreamExt, stream};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::Instrument;

/// Copy a format's `download` object from the streams response. No URL or headers
/// are accepted from callers; the server resolves a fresh reference itself.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize, utoipa::ToSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum DownloadSelection {
    Video {
        format_id: String,
        source_client: String,
        /// Distinguishes dubbed/multilingual formats that share an itag.
        audio_track_id: Option<String>,
    },
    Audio {
        format_id: String,
        source_client: String,
        audio_track_id: Option<String>,
    },
    Subtitle {
        source_client: String,
        track_id: String,
        language: String,
        ext: String,
    },
}

impl DownloadSelection {
    fn source_client(&self) -> &str {
        match self {
            Self::Video { source_client, .. }
            | Self::Audio { source_client, .. }
            | Self::Subtitle { source_client, .. } => source_client,
        }
    }

    // Preserve the media type, itag and exact audio track. Never fall back to
    // another language/default track, quality, codec, or a subtitle selection.
    fn mweb_fallback(&self) -> Option<Self> {
        if !matches!(self.source_client(), "IOS" | "TVHTML5") {
            return None;
        }
        let mut selection = self.clone();
        match &mut selection {
            Self::Audio { source_client, .. } | Self::Video { source_client, .. } => {
                *source_client = "MWEB".into();
                Some(selection)
            }
            Self::Subtitle { .. } => None,
        }
    }
    fn validate(&self) -> Result<(), ApiError> {
        let fields: Vec<&str> = match self {
            Self::Video {
                format_id,
                source_client,
                audio_track_id,
            }
            | Self::Audio {
                format_id,
                source_client,
                audio_track_id,
            } => {
                let mut fields = vec![format_id.as_str(), source_client.as_str()];
                fields.extend(audio_track_id.as_deref());
                fields
            }
            Self::Subtitle {
                source_client,
                track_id,
                language,
                ext,
            } => {
                vec![source_client, track_id, language, ext]
            }
        };
        if fields.iter().any(|field| {
            field.is_empty() || field.len() > 256 || field.chars().any(char::is_control)
        }) {
            return Err(ApiError::InvalidFilters(
                "download selection fields must contain 1–256 characters without control characters",
            ));
        }
        Ok(())
    }
}

pub(crate) struct DownloadSource {
    pub(crate) url: String,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) filename: String,
    pub(crate) content_type: String,
    pub(crate) is_subtitle: bool,
    pub(crate) chunked: bool,
    pub(crate) content_length: Option<u64>,
}

#[derive(Clone)]
pub(crate) struct DownloadService {
    extractor: StreamExtractor,
    client: Client,
    slots: Arc<Semaphore>,
    #[cfg(test)]
    test_origin: Option<Url>,
}

impl DownloadService {
    pub(crate) fn new(extractor: StreamExtractor) -> Self {
        Self {
            extractor,
            // Media transfers can take longer than the metadata client's total
            // timeout. Bound connection/idle reads instead of the whole file.
            client: Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(10))
                .read_timeout(Duration::from_secs(30))
                .build()
                .expect("valid download client configuration"),
            slots: Arc::new(Semaphore::new(4)),
            #[cfg(test)]
            test_origin: None,
        }
    }

    #[tracing::instrument(name = "download", skip_all)]
    pub(crate) async fn download(
        &self,
        video_id: &str,
        selection: DownloadSelection,
        headers: HeaderMap,
    ) -> Result<Response, ApiError> {
        selection.validate()?;
        let client = match selection.source_client() {
            "MWEB" => "MWEB",
            "IOS" => "IOS",
            "TVHTML5" => "TVHTML5",
            _ => "other",
        };
        let kind = match &selection {
            DownloadSelection::Video { .. } => "video",
            DownloadSelection::Audio { .. } => "audio",
            DownloadSelection::Subtitle { .. } => "subtitle",
        };
        let range = validated_range(&headers)?;
        tracing::info!(
            client,
            kind,
            range_requested = range.is_some(),
            "Resolving selected stream for download"
        );
        let permit = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| ApiError::DownloadBusy)?;
        let streams = if selection.source_client() == "MWEB" {
            self.extractor.extract_mweb(video_id).await?
        } else {
            self.extractor.extract_native(video_id).await?
        };
        let source = streams
            .select_download(&selection)
            .ok_or(ApiError::DownloadNotFound)?;
        tracing::info!(
            chunked = source.chunked,
            content_length = source.content_length,
            "Selected stream resolved"
        );
        if source.chunked {
            return self.chunked_download(source, range, permit).await;
        }
        let mut upstream = self.fetch(&source, range.as_ref()).await?;
        let status = upstream.status();
        tracing::info!(upstream_status = status.as_u16(), "Media server responded");
        let rejected_valid_range = status == StatusCode::RANGE_NOT_SATISFIABLE
            && range
                .as_ref()
                .zip(source.content_length)
                .is_some_and(|(range, total)| {
                    total > 0 && range_bounds(Some(range), total).is_some()
                });
        if (status == StatusCode::FORBIDDEN || rejected_valid_range)
            && let Some(fallback) = selection.mweb_fallback()
        {
            drop(upstream);
            tracing::warn!(
                client,
                upstream_status = status.as_u16(),
                "Native media rejected; trying the same format and track through MWEB"
            );
            let streams = self.extractor.extract_mweb(video_id).await?;
            let alternate = streams
                .select_download(&fallback)
                .filter(|alternate| {
                    alternate.content_type == source.content_type
                        && source
                            .content_length
                            .zip(alternate.content_length)
                            .is_none_or(|(original, alternate)| original == alternate)
                })
                .ok_or_else(|| {
                    tracing::warn!("No equivalent MWEB format and audio track available");
                    ApiError::DownloadRejected(status)
                })?;
            tracing::info!(
                client = "MWEB",
                "Equivalent stream resolved; using token-backed chunked download"
            );
            // Only retry before committing headers or bytes. The MWEB path
            // validates every range and never recursively retries another client.
            return self.chunked_download(alternate, range, permit).await;
        }
        if status == StatusCode::RANGE_NOT_SATISFIABLE && range.is_some() {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = status;
            if let Some(value) = upstream.headers().get(header::CONTENT_RANGE) {
                response
                    .headers_mut()
                    .insert(header::CONTENT_RANGE, value.clone());
            }
            return Ok(response);
        }
        if status != StatusCode::OK && !(status == StatusCode::PARTIAL_CONTENT && range.is_some()) {
            return Err(ApiError::DownloadRejected(status));
        }
        if upstream
            .headers()
            .get(header::CONTENT_ENCODING)
            .is_some_and(|encoding| encoding != "identity")
        {
            return Err(ApiError::DownloadUpstream);
        }
        let mut response_headers = HeaderMap::new();
        for name in [
            header::CONTENT_LENGTH,
            header::CONTENT_RANGE,
            header::ACCEPT_RANGES,
        ] {
            if let Some(value) = upstream.headers().get(&name) {
                response_headers.insert(name, value.clone());
            }
        }
        response_headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str(&source.content_type).map_err(|_| ApiError::DownloadUpstream)?,
        );
        response_headers.insert(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_str(&format!("attachment; filename=\"{}\"", source.filename))
                .map_err(|_| ApiError::DownloadUpstream)?,
        );
        response_headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        response_headers.insert("x-download-source-client", HeaderValue::from_static(client));
        response_headers.insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );

        // Read the first chunk before committing success headers, so immediate
        // body/timeout failures can still return the documented JSON error.
        let first = upstream.chunk().await.map_err(download_http_error)?;
        tracing::info!("Streaming download body");
        let span = tracing::Span::current();
        let chunks = futures_util::stream::try_unfold(
            (upstream, permit, first),
            move |(mut upstream, permit, first)| {
                let span = span.clone();
                async move {
                    let chunk = match first {
                        Some(chunk) => Some(chunk),
                        None => upstream
                            .chunk()
                            .await
                            .map_err(|_| std::io::Error::other("upstream download interrupted"))?,
                    };
                    Ok::<_, std::io::Error>(chunk.map(|chunk| (chunk, (upstream, permit, None))))
                }
                .instrument(span)
            },
        );
        let mut response = Response::new(Body::from_stream(chunks));
        *response.status_mut() = status;
        *response.headers_mut() = response_headers;
        Ok(response)
    }

    #[tracing::instrument(name = "download.chunked", skip_all)]
    async fn chunked_download(
        &self,
        source: DownloadSource,
        range: Option<HeaderValue>,
        permit: OwnedSemaphorePermit,
    ) -> Result<Response, ApiError> {
        let total = source
            .content_length
            .filter(|length| *length > 0)
            .ok_or(ApiError::DownloadUpstream)?;
        let bounds = range_bounds(range.as_ref(), total);
        let Some((start, end)) = bounds else {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::RANGE_NOT_SATISFIABLE;
            response.headers_mut().insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes */{total}")).unwrap(),
            );
            return Ok(response);
        };
        tracing::info!(
            start,
            end,
            total,
            chunk_size = CHUNK_SIZE,
            concurrency = 8,
            "Starting chunked download"
        );
        let source = Arc::new(source);
        let first_end = end.min(start.saturating_add(CHUNK_SIZE - 1));
        let first = self.fetch_chunk(&source, start, first_end, total).await?;
        let service = self.clone();
        let fetch_source = source.clone();
        let span = tracing::Span::current();
        let rest = stream::iter((first_end + 1..=end).step_by(CHUNK_SIZE as usize))
            .map(move |start| {
                let service = service.clone();
                let source = fetch_source.clone();
                let span = span.clone();
                async move {
                    service
                        .fetch_chunk(
                            &source,
                            start,
                            end.min(start.saturating_add(CHUNK_SIZE - 1)),
                            total,
                        )
                        .await
                        .map_err(|error| {
                            tracing::warn!(failure = ?error, start, "Chunk download failed");
                            std::io::Error::other("upstream chunk download interrupted")
                        })
                }
                .instrument(span)
            })
            .buffered(8);
        let chunks = stream::once(async move { Ok::<_, std::io::Error>(first) })
            .chain(rest)
            .map(move |chunk| {
                let _ = &permit;
                chunk
            });
        let mut response = Response::new(Body::from_stream(chunks));
        *response.status_mut() = if range.is_some() {
            StatusCode::PARTIAL_CONTENT
        } else {
            StatusCode::OK
        };
        let headers = response.headers_mut();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_str(&source.content_type).map_err(|_| ApiError::DownloadUpstream)?,
        );
        headers.insert(
            header::CONTENT_DISPOSITION,
            HeaderValue::from_str(&format!("attachment; filename=\"{}\"", source.filename))
                .map_err(|_| ApiError::DownloadUpstream)?,
        );
        headers.insert(
            header::CONTENT_LENGTH,
            HeaderValue::from_str(&(end - start + 1).to_string()).unwrap(),
        );
        headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        headers.insert("x-download-source-client", HeaderValue::from_static("MWEB"));
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        headers.insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
        if range.is_some() {
            headers.insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes {start}-{end}/{total}")).unwrap(),
            );
        }
        Ok(response)
    }

    #[tracing::instrument(
        name = "download.chunk",
        level = "debug",
        skip_all,
        fields(start = start, end = end, total = total)
    )]
    async fn fetch_chunk(
        &self,
        source: &DownloadSource,
        start: u64,
        end: u64,
        total: u64,
    ) -> Result<axum::body::Bytes, ApiError> {
        let range = HeaderValue::from_str(&format!("bytes={start}-{end}")).unwrap();
        let mut response = self.fetch(source, Some(&range)).await?;
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(if response.status().is_success() {
                ApiError::DownloadUpstream
            } else {
                ApiError::DownloadRejected(response.status())
            });
        }
        let expected_range = format!("bytes {start}-{end}/{total}");
        let length = end - start + 1;
        if response
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|value| value.to_str().ok())
            != Some(expected_range.as_str())
            || response
                .headers()
                .get(header::CONTENT_ENCODING)
                .is_some_and(|value| value != "identity")
            || response.content_length().is_some_and(|size| size != length)
        {
            return Err(ApiError::DownloadUpstream);
        }
        let mut bytes = Vec::with_capacity(length as usize);
        while let Some(chunk) = response.chunk().await.map_err(download_http_error)? {
            if chunk.len() > (length as usize).saturating_sub(bytes.len()) {
                return Err(ApiError::DownloadUpstream);
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != length as usize {
            return Err(ApiError::DownloadUpstream);
        }
        tracing::debug!(bytes = bytes.len(), "Chunk verified");
        Ok(bytes.into())
    }

    #[tracing::instrument(name = "download.upstream", level = "debug", skip_all)]
    async fn fetch(
        &self,
        source: &DownloadSource,
        range: Option<&HeaderValue>,
    ) -> Result<reqwest::Response, ApiError> {
        let mut url = Url::parse(&source.url).map_err(|_| ApiError::DownloadUpstream)?;
        for attempt in 0..=5 {
            if !self.allowed_url(&url, source.is_subtitle) {
                return Err(ApiError::DownloadUpstream);
            }
            let mut request = self
                .client
                .get(url.clone())
                .header(header::ACCEPT_ENCODING, "identity");
            for (key, value) in &source.headers {
                if key.eq_ignore_ascii_case("user-agent") || key.eq_ignore_ascii_case("referer") {
                    request = request.header(key, value);
                }
            }
            if let Some(range) = range {
                request = request.header(header::RANGE, range);
            }
            let response = request.send().await.map_err(download_http_error)?;
            tracing::debug!(
                upstream_status = response.status().as_u16(),
                attempt,
                "Download upstream responded"
            );
            if response.status().is_redirection() {
                if attempt == 5 {
                    return Err(ApiError::DownloadUpstream);
                }
                let location = response
                    .headers()
                    .get(header::LOCATION)
                    .and_then(|value| value.to_str().ok())
                    .ok_or(ApiError::DownloadUpstream)?;
                url = url.join(location).map_err(|_| ApiError::DownloadUpstream)?;
                continue;
            }
            return Ok(response);
        }
        Err(ApiError::DownloadUpstream)
    }

    fn allowed_url(&self, url: &Url, subtitle: bool) -> bool {
        #[cfg(test)]
        if self
            .test_origin
            .as_ref()
            .is_some_and(|origin| url.origin() == origin.origin())
        {
            return true;
        }
        let host = url.host_str().unwrap_or_default();
        url.scheme() == "https"
            && url.port_or_known_default() == Some(443)
            && url.username().is_empty()
            && url.password().is_none()
            && if subtitle {
                (host == "youtube.com"
                    || host.ends_with(".youtube.com")
                    || host == "video.google.com")
                    && url.path() == "/api/timedtext"
            } else {
                host.ends_with(".googlevideo.com")
            }
    }
}

const CHUNK_SIZE: u64 = 1024 * 1024;

// validated_range has already checked syntax and arithmetic before extraction.
fn range_bounds(range: Option<&HeaderValue>, total: u64) -> Option<(u64, u64)> {
    let Some(range) = range else {
        return Some((0, total - 1));
    };
    let (start, end) = range
        .to_str()
        .ok()?
        .strip_prefix("bytes=")?
        .split_once('-')?;
    if start.is_empty() {
        let suffix = end.parse::<u64>().ok()?;
        Some((total.saturating_sub(suffix), total - 1))
    } else {
        let start = start.parse::<u64>().ok()?;
        let end = if end.is_empty() {
            total - 1
        } else {
            end.parse::<u64>().ok()?.min(total - 1)
        };
        (start <= end).then_some((start, end))
    }
}

fn validated_range(headers: &HeaderMap) -> Result<Option<HeaderValue>, ApiError> {
    if headers.get_all(header::RANGE).iter().count() > 1 {
        return Err(ApiError::InvalidFilters("only one byte range is supported"));
    }
    let Some(value) = headers.get(header::RANGE) else {
        return Ok(None);
    };
    let invalid = || {
        ApiError::InvalidFilters(
            "Range must be a single byte range such as bytes=0-1023, bytes=1024-, or bytes=-1024",
        )
    };
    let text = value.to_str().map_err(|_| invalid())?;
    let (start, end) = text
        .strip_prefix("bytes=")
        .and_then(|text| text.split_once('-'))
        .ok_or_else(invalid)?;
    let number = |value: &str| -> Result<Option<u64>, ApiError> {
        if value.is_empty() {
            return Ok(None);
        }
        if !value.bytes().all(|c| c.is_ascii_digit()) {
            return Err(invalid());
        }
        value.parse().map(Some).map_err(|_| invalid())
    };
    match (number(start)?, number(end)?) {
        (None, None | Some(0)) => return Err(invalid()),
        (Some(start), Some(end)) if start > end => return Err(invalid()),
        _ => {}
    }
    Ok(Some(value.clone()))
}

fn download_http_error(error: reqwest::Error) -> ApiError {
    if error.is_timeout() {
        ApiError::DownloadTimeout
    } else {
        ApiError::DownloadUpstream
    }
}

#[cfg(test)]
mod chunk_tests;
#[cfg(test)]
mod fallback_tests;
#[cfg(test)]
mod tests;
