use crate::ApiError;
use crate::downloads::{DownloadSelection, DownloadSource};
use crate::mweb::MwebExtractor;
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Semaphore;

const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Copy)]
struct PlayerClient {
    name: &'static str,
    version: &'static str,
    id: &'static str,
    user_agent: &'static str,
}

// YouTube can retire these profiles independently of the public Data API.
const IOS: PlayerClient = PlayerClient {
    name: "IOS",
    version: "20.10.4",
    id: "5",
    user_agent: "com.google.ios.youtube/20.10.4 (iPhone16,2; U; CPU iOS 18_3_2 like Mac OS X;)",
};
const TV: PlayerClient = PlayerClient {
    name: "TVHTML5",
    version: "5.20260707",
    id: "7",
    user_agent: "Mozilla/5.0 (ChromiumStylePlatform) Cobalt/Version",
};
const MWEB: PlayerClient = PlayerClient {
    name: "MWEB",
    version: crate::mweb::VERSION,
    id: "2",
    user_agent: crate::mweb::USER_AGENT,
};

/// In-process extraction with native HTTP clients and a embedded deno_core pool for MWEB.
/// Clones share pools, parsed-player cache, and concurrency limits.
#[derive(Clone)]
pub struct StreamExtractor {
    client: Client,
    player_url: Url,
    timeout: Duration,
    slots: Arc<Semaphore>,
    mweb: Option<MwebExtractor>,
}

impl StreamExtractor {
    #[cfg(test)]
    pub(crate) fn for_test(player_url: Url) -> Self {
        let mut extractor = Self::new(Client::new());
        extractor.player_url = player_url;
        extractor.mweb = None;
        extractor
    }

    pub fn new(client: Client) -> Self {
        Self {
            client,
            player_url: Url::parse("https://www.youtube.com/youtubei/v1/player")
                .expect("valid player URL"),
            timeout: Duration::from_secs(120),
            slots: Arc::new(Semaphore::new(4)),
            mweb: Some(MwebExtractor::default()),
        }
    }

    #[tracing::instrument(name = "streams.extract", skip_all, fields(video_id))]
    pub(crate) async fn extract(&self, video_id: &str) -> Result<StreamsResponse, ApiError> {
        if video_id.len() != 11
            || !video_id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
        {
            return Err(ApiError::InvalidFilters(
                "video_id must contain exactly 11 letters, digits, underscores, or hyphens",
            ));
        }
        tracing::Span::current().record("video_id", video_id);
        tracing::info!("Extracting available streams");
        let _permit = self.slots.try_acquire().map_err(|_| ApiError::StreamBusy)?;
        let work = async {
            let (ios, tv, mweb) = tokio::join!(
                self.player(video_id, IOS),
                self.player(video_id, TV),
                async {
                    match &self.mweb {
                        Some(extractor) => Some(extractor.extract(video_id).await),
                        None => None,
                    }
                }
            );
            let mut response = StreamsResponse::empty(video_id);
            let mut streams = BTreeSet::new();
            let mut subtitles = BTreeSet::new();
            let mut timed_out = false;
            for (profile, result) in [(IOS, ios), (TV, tv)] {
                match result {
                    Ok(player) => {
                        response.add_player(player, profile, &mut streams, &mut subtitles)
                    }
                    Err(error) => {
                        tracing::warn!(
                            client = profile.name,
                            timeout = matches!(error, ApiError::StreamTimeout),
                            "Player extraction failed"
                        );
                        timed_out |= matches!(error, ApiError::StreamTimeout);
                        response.warnings.insert(format!(
                            "{} player request failed; its formats may be unavailable",
                            profile.name
                        ));
                    }
                }
            }
            if let Some(mweb) = mweb {
                match mweb {
                    Ok(raw) => {
                        if raw["_skipped_formats"].as_u64().unwrap_or(0) > 0 {
                            response.warnings.insert("Some MWEB formats had no usable media URL or required unsupported player transforms and were skipped".into());
                        }
                        if raw["playabilityStatus"]["status"] == "OK" {
                            response.extractor = "native_innertube_deno_core";
                            response.coverage = "native_and_mweb_formats";
                        }
                        match serde_json::from_value(raw) {
                            Ok(player) => {
                                response.add_player(player, MWEB, &mut streams, &mut subtitles)
                            }
                            Err(_) => {
                                tracing::warn!(client = "MWEB", "Invalid player response");
                                response
                                    .warnings
                                    .insert("Invalid MWEB player response".into());
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            client = "MWEB",
                            timeout = matches!(error, ApiError::StreamTimeout),
                            "MWEB extraction failed"
                        );
                        timed_out |= matches!(error, ApiError::StreamTimeout);
                        response.warnings.insert("MWEB extraction or proof-of-origin attestation failed in the embedded runtime; its formats are unavailable".into());
                    }
                }
            }
            if response.video_streams.is_empty()
                && response.audio_streams.is_empty()
                && response.manifests.is_empty()
                && response.subtitles.is_empty()
            {
                return Err(if timed_out {
                    ApiError::StreamTimeout
                } else {
                    ApiError::StreamExtraction
                });
            }
            response.log_summary();
            Ok(response)
        };
        tokio::time::timeout(self.timeout, work)
            .await
            .map_err(|_| ApiError::StreamTimeout)?
    }

    #[tracing::instrument(name = "streams.extract_mweb", skip_all, fields(video_id))]
    pub(crate) async fn extract_mweb(&self, video_id: &str) -> Result<StreamsResponse, ApiError> {
        if video_id.len() != 11
            || !video_id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
        {
            return Err(ApiError::InvalidFilters(
                "video_id must contain exactly 11 letters, digits, underscores, or hyphens",
            ));
        }
        tracing::Span::current().record("video_id", video_id);
        let _permit = self.slots.try_acquire().map_err(|_| ApiError::StreamBusy)?;
        let player = tokio::time::timeout(self.timeout, self.mweb_player(video_id))
            .await
            .map_err(|_| ApiError::StreamTimeout)??;
        let mut response = StreamsResponse::empty(video_id);
        response.extractor = "native_innertube_deno_core";
        response.coverage = "native_and_mweb_formats";
        response.add_player(player, MWEB, &mut BTreeSet::new(), &mut BTreeSet::new());
        response.log_summary();
        Ok(response)
    }

    async fn mweb_player(&self, video_id: &str) -> Result<PlayerResponse, ApiError> {
        // Local mock servers supply an already-resolved MWEB player response;
        // production always runs BotGuard and URL transforms inside embedded V8 isolates.
        #[cfg(test)]
        if self.mweb.is_none() {
            return self.player(video_id, MWEB).await;
        }
        let extractor = self.mweb.as_ref().ok_or(ApiError::MwebExtraction)?;
        let raw = extractor.extract(video_id).await?;
        serde_json::from_value(raw).map_err(|_| ApiError::MwebExtraction)
    }

    pub(crate) async fn extract_native(&self, video_id: &str) -> Result<StreamsResponse, ApiError> {
        let mut native = self.clone();
        native.mweb = None;
        native.extract(video_id).await
    }

    #[tracing::instrument(name = "streams.player", skip_all, fields(client = profile.name))]
    async fn player(
        &self,
        video_id: &str,
        profile: PlayerClient,
    ) -> Result<PlayerResponse, ApiError> {
        tracing::info!("Requesting player metadata");
        let mut client = json!({"clientName": profile.name, "clientVersion": profile.version, "hl": "en", "gl": "US"});
        if profile.name == "IOS" {
            client["deviceMake"] = json!("Apple");
            client["deviceModel"] = json!("iPhone16,2");
        }
        let body = json!({"context": {"client": client}, "videoId": video_id, "contentCheckOk": true, "racyCheckOk": true});
        let mut response = self
            .client
            .post(self.player_url.clone())
            .query(&[("prettyPrint", "false")])
            .header("User-Agent", profile.user_agent)
            .header("X-Youtube-Client-Name", profile.id)
            .header("X-Youtube-Client-Version", profile.version)
            .json(&body)
            .send()
            .await
            .map_err(player_http_error)?;
        tracing::info!(
            upstream_status = response.status().as_u16(),
            "Player responded"
        );
        response = response.error_for_status().map_err(player_http_error)?;
        if response
            .content_length()
            .is_some_and(|size| size > MAX_RESPONSE_BYTES as u64)
        {
            return Err(ApiError::StreamExtraction);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(player_http_error)? {
            if chunk.len() > MAX_RESPONSE_BYTES.saturating_sub(bytes.len()) {
                return Err(ApiError::StreamExtraction);
            }
            bytes.extend_from_slice(&chunk);
        }
        let player: PlayerResponse =
            serde_json::from_slice(&bytes).map_err(|_| ApiError::StreamExtraction)?;
        if player
            .video_details
            .as_ref()
            .is_some_and(|details| details.video_id != video_id)
        {
            return Err(ApiError::StreamExtraction);
        }
        Ok(player)
    }
}

fn player_http_error(error: reqwest::Error) -> ApiError {
    if error.is_timeout() {
        ApiError::StreamTimeout
    } else {
        ApiError::StreamExtraction
    }
}

#[derive(Serialize, utoipa::ToSchema)]
pub(crate) struct StreamsResponse {
    video_id: String,
    title: Option<String>,
    duration_seconds: Option<u64>,
    extractor: &'static str,
    /// Native player responses, not every browser-only format.
    coverage: &'static str,
    urls_are_temporary: bool,
    /// Shortest lifetime reported by a successful client.
    expires_in_seconds: Option<u64>,
    /// Video-only and combined video/audio formats; inspect has_audio.
    video_streams: Vec<StreamFormat>,
    audio_streams: Vec<StreamFormat>,
    /// Master manifests for HLS/DASH; variant playlists are not expanded.
    manifests: Vec<Manifest>,
    subtitles: Vec<SubtitleTrack>,
    /// Nonempty when clients failed or unsupported formats were skipped.
    warnings: BTreeSet<String>,
}

#[derive(Serialize, utoipa::ToSchema)]
struct StreamFormat {
    /// Send this object to POST /videos/{video_id}/download.
    download: DownloadSelection,
    format_id: String,
    url: String,
    mime_type: String,
    codecs: Vec<String>,
    has_video: bool,
    has_audio: bool,
    width: Option<u64>,
    height: Option<u64>,
    fps: Option<f64>,
    quality_label: Option<String>,
    /// Bits per second.
    bitrate: Option<u64>,
    content_length: Option<u64>,
    duration_ms: Option<u64>,
    source_client: String,
    http_headers: BTreeMap<String, String>,
    /// Native properties, including audioTrack, colorInfo, and byte ranges.
    metadata: Value,
}

#[derive(Serialize, utoipa::ToSchema)]
struct Manifest {
    kind: ManifestKind,
    url: String,
    source_client: String,
    http_headers: BTreeMap<String, String>,
}

#[derive(Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
enum ManifestKind {
    Hls,
    Dash,
}

#[derive(Serialize, utoipa::ToSchema)]
struct SubtitleTrack {
    language: String,
    name: String,
    kind: SubtitleKind,
    /// Machine translation from source_language into language.
    is_translation: bool,
    source_language: String,
    source_client: String,
    formats: Vec<SubtitleFormat>,
}

#[derive(Clone, Copy, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
enum SubtitleKind {
    Manual,
    Automatic,
}

#[derive(Serialize, utoipa::ToSchema)]
struct SubtitleFormat {
    /// Send this object to POST /videos/{video_id}/download.
    download: DownloadSelection,
    ext: String,
    url: String,
    http_headers: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlayerResponse {
    playability_status: PlayabilityStatus,
    video_details: Option<PlayerVideoDetails>,
    streaming_data: Option<StreamingData>,
    captions: Option<Value>,
}

#[derive(Deserialize)]
struct PlayabilityStatus {
    status: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PlayerVideoDetails {
    video_id: String,
    title: Option<String>,
    length_seconds: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StreamingData {
    #[serde(default)]
    formats: Vec<Value>,
    #[serde(default)]
    adaptive_formats: Vec<Value>,
    expires_in_seconds: Option<String>,
    hls_manifest_url: Option<String>,
    dash_manifest_url: Option<String>,
    server_abr_streaming_url: Option<String>,
}

impl StreamsResponse {
    fn log_summary(&self) {
        tracing::info!(
            video_formats = self.video_streams.len(),
            audio_formats = self.audio_streams.len(),
            subtitle_tracks = self.subtitles.len(),
            manifests = self.manifests.len(),
            warnings = self.warnings.len(),
            "Stream extraction completed"
        );
    }
    pub(crate) fn select_download(self, selection: &DownloadSelection) -> Option<DownloadSource> {
        for stream in self.video_streams.into_iter().chain(self.audio_streams) {
            if &stream.download == selection {
                let ext = match stream.mime_type.split(';').next()?.trim() {
                    "video/mp4" => "mp4",
                    "audio/mp4" => "m4a",
                    "video/webm" | "audio/webm" => "webm",
                    "audio/mpeg" => "mp3",
                    "audio/ogg" => "ogg",
                    _ => "bin",
                };
                return Some(DownloadSource {
                    url: stream.url,
                    headers: stream.http_headers,
                    filename: format!(
                        "{}-{}.{}",
                        self.video_id,
                        safe_filename(&stream.format_id),
                        ext
                    ),
                    content_type: stream.mime_type,
                    is_subtitle: false,
                    chunked: stream.source_client == "MWEB",
                    content_length: stream.content_length,
                });
            }
        }
        for track in self.subtitles {
            for format in track.formats {
                if &format.download == selection {
                    let content_type = match format.ext.as_str() {
                        "vtt" => "text/vtt; charset=utf-8",
                        "json3" => "application/json",
                        "ttml" => "application/ttml+xml",
                        _ => "application/xml",
                    };
                    return Some(DownloadSource {
                        url: format.url,
                        headers: format.http_headers,
                        filename: format!(
                            "{}-{}.{}",
                            self.video_id,
                            safe_filename(&track.language),
                            format.ext
                        ),
                        content_type: content_type.into(),
                        is_subtitle: true,
                        chunked: false,
                        content_length: None,
                    });
                }
            }
        }
        None
    }

    fn empty(video_id: &str) -> Self {
        Self {
            video_id: video_id.into(),
            title: None,
            duration_seconds: None,
            extractor: "native_innertube",
            coverage: "native_client_formats",
            urls_are_temporary: true,
            expires_in_seconds: None,
            video_streams: Vec::new(),
            audio_streams: Vec::new(),
            manifests: Vec::new(),
            subtitles: Vec::new(),
            warnings: BTreeSet::new(),
        }
    }

    fn add_player(
        &mut self,
        player: PlayerResponse,
        profile: PlayerClient,
        streams: &mut BTreeSet<String>,
        subtitles: &mut BTreeSet<String>,
    ) {
        if player.playability_status.status != "OK" {
            self.warnings.insert(format!(
                "{} player did not grant playback; its formats may be unavailable",
                profile.name
            ));
            return;
        }
        if let Some(details) = player.video_details {
            self.title = self.title.take().or(details.title);
            self.duration_seconds = self.duration_seconds.or_else(|| {
                details
                    .length_seconds
                    .and_then(|duration| duration.parse().ok())
            });
        }
        if let Some(data) = player.streaming_data {
            if let Some(lifetime) = data
                .expires_in_seconds
                .and_then(|value| value.parse::<u64>().ok())
            {
                self.expires_in_seconds = Some(
                    self.expires_in_seconds
                        .map_or(lifetime, |previous| previous.min(lifetime)),
                );
            }
            if data.server_abr_streaming_url.is_some() {
                self.warnings.insert("SABR streaming requires a dedicated protocol implementation and is not exposed as a playable URL".into());
            }
            for (kind, url) in [
                (ManifestKind::Hls, data.hls_manifest_url),
                (ManifestKind::Dash, data.dash_manifest_url),
            ] {
                if let Some(url) = url.filter(|url| http_url(url).is_some())
                    && !self.manifests.iter().any(|manifest| manifest.url == url)
                {
                    self.manifests.push(Manifest {
                        kind,
                        url,
                        source_client: profile.name.into(),
                        http_headers: headers(profile),
                    });
                }
            }
            for raw in data.formats.into_iter().chain(data.adaptive_formats) {
                if let Some(stream) = stream_format(raw, profile) {
                    let key = format!("{}:{}", profile.name, stream.url);
                    if streams.insert(key) {
                        if stream.has_video {
                            self.video_streams.push(stream);
                        } else if stream.has_audio {
                            self.audio_streams.push(stream);
                        }
                    }
                } else {
                    self.warnings.insert("Some format URLs were absent, invalid, DRM-protected, or required unsupported JavaScript signature/n-parameter decoding".into());
                }
            }
        }
        if let Some(captions) = player.captions {
            self.add_subtitles(&captions, profile, subtitles);
        }
    }

    fn add_subtitles(
        &mut self,
        captions: &Value,
        profile: PlayerClient,
        seen: &mut BTreeSet<String>,
    ) {
        let Some(renderer) = captions.get("playerCaptionsTracklistRenderer") else {
            return;
        };
        let translations = renderer["translationLanguages"].as_array();
        for track in renderer["captionTracks"].as_array().into_iter().flatten() {
            let (Some(base), Some(language)) = (
                track["baseUrl"].as_str().and_then(http_url),
                track["languageCode"].as_str(),
            ) else {
                continue;
            };
            let automatic = track["kind"].as_str() == Some("asr");
            let track_id = track["vssId"].as_str().unwrap_or(language);
            let name = text(&track["name"]).unwrap_or_else(|| language.into());
            let mut targets = vec![(language.to_owned(), name, false)];
            if track["isTranslatable"].as_bool() == Some(true) {
                for target in translations.into_iter().flatten() {
                    if let Some(code) = target["languageCode"]
                        .as_str()
                        .filter(|code| *code != language)
                    {
                        targets.push((
                            code.into(),
                            text(&target["languageName"]).unwrap_or_else(|| code.into()),
                            true,
                        ));
                    }
                }
            }
            for (target, name, translated) in targets {
                let key = format!("{track_id}:{target}:{automatic}:{translated}");
                if !seen.insert(key) {
                    continue;
                }
                let formats = ["vtt", "ttml", "srv1", "srv2", "srv3", "json3"]
                    .into_iter()
                    .map(|ext| {
                        let mut url = base.clone();
                        let pairs: Vec<_> = url
                            .query_pairs()
                            .filter(|(key, _)| key != "fmt" && key != "tlang")
                            .map(|(key, value)| (key.into_owned(), value.into_owned()))
                            .collect();
                        url.set_query(None);
                        url.query_pairs_mut()
                            .extend_pairs(pairs)
                            .append_pair("fmt", ext);
                        if translated {
                            url.query_pairs_mut().append_pair("tlang", &target);
                        }
                        SubtitleFormat {
                            download: DownloadSelection::Subtitle {
                                source_client: profile.name.into(),
                                track_id: track_id.into(),
                                language: target.clone(),
                                ext: ext.into(),
                            },
                            ext: ext.into(),
                            url: url.into(),
                            http_headers: headers(profile),
                        }
                    })
                    .collect();
                self.subtitles.push(SubtitleTrack {
                    language: target,
                    name,
                    kind: if automatic || translated {
                        SubtitleKind::Automatic
                    } else {
                        SubtitleKind::Manual
                    },
                    is_translation: translated,
                    source_language: language.into(),
                    source_client: profile.name.into(),
                    formats,
                });
            }
        }
    }
}

fn headers(profile: PlayerClient) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("User-Agent".into(), profile.user_agent.into()),
        ("Referer".into(), "https://www.youtube.com/".into()),
    ])
}

fn safe_filename(value: &str) -> String {
    value
        .chars()
        .take(80)
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn text(value: &Value) -> Option<String> {
    value["simpleText"].as_str().map(str::to_owned).or_else(|| {
        value["runs"]
            .as_array()
            .map(|runs| runs.iter().filter_map(|run| run["text"].as_str()).collect())
    })
}

fn number(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn http_url(value: &str) -> Option<Url> {
    Url::parse(value).ok().filter(|url| {
        matches!(url.scheme(), "https" | "http")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
    })
}

fn format_url(raw: &Value) -> Option<Url> {
    let mut url = if let Some(url) = raw["url"].as_str() {
        http_url(url)?
    } else {
        let cipher = raw["signatureCipher"]
            .as_str()
            .or_else(|| raw["cipher"].as_str())?;
        let params: BTreeMap<_, _> = Url::parse(&format!("https://cipher.invalid/?{cipher}"))
            .ok()?
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        if params.get("s").is_some_and(|s| !s.is_empty()) {
            return None;
        }
        let mut url = http_url(params.get("url")?)?;
        if let Some(signature) = params.get("sig").or_else(|| params.get("signature")) {
            url.query_pairs_mut().append_pair(
                params.get("sp").map(String::as_str).unwrap_or("signature"),
                signature,
            );
        }
        url
    };
    // n needs a player-JavaScript transformation. Do not present unresolved URLs
    // as playback-ready or return encrypted cipher strings as media URLs.
    if url.query_pairs().any(|(key, _)| key == "n") {
        return None;
    }
    url.set_fragment(None);
    Some(url)
}

fn stream_format(raw: Value, profile: PlayerClient) -> Option<StreamFormat> {
    if raw["drmFamilies"]
        .as_array()
        .is_some_and(|families| !families.is_empty())
        || raw
            .get("drmTrackType")
            .is_some_and(|value| !value.is_null())
    {
        return None;
    }
    let format_id = number(&raw["itag"])?.to_string();
    let url = if profile.name == "MWEB" {
        raw["url"].as_str().and_then(http_url)?
    } else {
        format_url(&raw)?
    };
    let mime_type = raw["mimeType"].as_str()?.to_owned();
    let codecs: Vec<String> = mime_type
        .split(';')
        .find_map(|part| part.trim().strip_prefix("codecs="))
        .map(|codecs| {
            codecs
                .trim_matches('"')
                .split(',')
                .map(|codec| codec.trim().to_owned())
                .collect()
        })
        .unwrap_or_default();
    let has_video = mime_type.starts_with("video/");
    let has_audio = mime_type.starts_with("audio/") || (has_video && codecs.len() > 1);
    if !has_video && !has_audio {
        return None;
    }
    Some(StreamFormat {
        download: if has_video {
            DownloadSelection::Video {
                format_id: format_id.clone(),
                source_client: profile.name.into(),
                audio_track_id: raw["audioTrack"]["id"].as_str().map(str::to_owned),
            }
        } else {
            DownloadSelection::Audio {
                format_id: format_id.clone(),
                source_client: profile.name.into(),
                audio_track_id: raw["audioTrack"]["id"].as_str().map(str::to_owned),
            }
        },
        format_id,
        url: url.into(),
        codecs,
        has_video,
        has_audio,
        mime_type,
        width: number(&raw["width"]),
        height: number(&raw["height"]),
        fps: raw["fps"].as_f64(),
        quality_label: raw["qualityLabel"].as_str().map(str::to_owned),
        bitrate: number(&raw["bitrate"]),
        content_length: number(&raw["contentLength"]),
        duration_ms: number(&raw["approxDurationMs"]),
        source_client: profile.name.into(),
        http_headers: headers(profile),
        metadata: raw,
    })
}

#[cfg(test)]
mod tests;
