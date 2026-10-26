//! MWEB attestation and URL extraction. Rust owns HTTP; pooled embedded V8 isolates run JS.
mod embedded;
use crate::ApiError;
use embedded::{EmbeddedPool, Session};
use regex::Regex;
use reqwest::{Client, Response, Url};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tracing::Instrument;

pub(crate) const USER_AGENT: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36(KHTML, like Gecko)";
pub(crate) const VERSION: &str = "2.20260205.04.01";
const MAX_BYTES: u64 = 32 * 1024 * 1024;
const WORK_TIMEOUT: Duration = Duration::from_secs(110);

#[derive(Clone)]
struct PlayerScript {
    url: String,
    timestamp: u64,
    script: String,
}

#[derive(Clone)]
pub(crate) struct MwebExtractor {
    pool: EmbeddedPool,
    script: Arc<Mutex<Option<PlayerScript>>>,
    client: Client,
}
impl Default for MwebExtractor {
    fn default() -> Self {
        Self {
            pool: EmbeddedPool::default(),
            script: Arc::default(),
            client: Client::builder()
                .user_agent(USER_AGENT)
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(10))
                .timeout(Duration::from_secs(30))
                .build()
                .expect("valid MWEB HTTP client configuration"),
        }
    }
}
impl MwebExtractor {
    #[tracing::instrument(name = "mweb.extract", skip_all)]
    pub(crate) async fn extract(&self, video_id: &str) -> Result<Value, ApiError> {
        let started = Instant::now();
        let work = async {
            let mut session = self.pool.acquire().await?;
            let http = Http {
                client: self.client.clone(),
                deadline: Instant::now() + WORK_TIMEOUT,
            };
            let result = self
                .extract_with_session(video_id, &http, &mut session)
                .await;
            // Failed/cancelled sessions interrupt and dispose their isolate;
            // no partially initialized request state returns to the pool.
            if result.is_ok() {
                session.recycle().await?;
            }
            result
        };
        let result = tokio::time::timeout(WORK_TIMEOUT, work)
            .await
            .unwrap_or(Err(Failure::Timeout));
        tracing::info!(
            elapsed_ms = started.elapsed().as_millis() as u64,
            success = result.is_ok(),
            "MWEB extraction finished"
        );
        result.map_err(|error| {
            tracing::warn!(failure = ?error, "MWEB extraction failed");
            match error {
                Failure::Busy => ApiError::StreamBusy,
                Failure::Attestation => ApiError::MwebAttestation,
                Failure::Timeout => ApiError::StreamTimeout,
                Failure::Http | Failure::Runtime => ApiError::MwebExtraction,
            }
        })
    }
    async fn extract_with_session(
        &self,
        video_id: &str,
        http: &Http,
        session: &mut Session,
    ) -> Result<Value> {
        let (config, challenge) = stage("homepage", async {
            let html = http.text("https://www.youtube.com/").await?;
            homepage(&html)
        })
        .await?;
        stage("embedded_runtime", session.reset()).await?;
        let interpreter_url = session.js(&format!(
            "globalThis.yt={{config_:{config}}};window.yt=yt;globalThis.__att=({challenge}).R;globalThis.__challenge=(typeof __att==='string'?JSON.parse(__att):__att).bgChallenge;__challenge.interpreterUrl.privateDoNotAccessOrElseTrustedResourceUrlWrappedValue"
        )).await?;
        stage("botguard_interpreter", async {
            let url = Url::parse("https://www.youtube.com/")
                .unwrap()
                .join(&interpreter_url)
                .map_err(|_| Failure::Http)?;
            let interpreter = http.text(url.as_str()).await?;
            session.js(&interpreter).await?;
            Ok(())
        })
        .await?;
        let snapshot = stage("botguard_snapshot", async {
            session.js(r#"
                globalThis.__signals=[];globalThis.__snapshotResult=null;
                globalThis.__vm=globalThis[__challenge.globalName];
                __vm.a(__challenge.program,(snapshot,shutdown)=>{globalThis.__snapshot=snapshot;globalThis.__shutdown=shutdown;},true,undefined,()=>{},[[],[]],undefined,false,[()=>{},()=>{},()=>{},()=>{},()=>{}]);
                (0,__snapshot)(result=>{globalThis.__snapshotResult=result;},[undefined,undefined,__signals,undefined]);
            "#).await?;
            let snapshot = session.js("__snapshotResult").await?;
            if snapshot == "null" || session.js("typeof __signals[0]").await? != "function" { return Err(Failure::Attestation); }
            Ok(snapshot)
        }).await?;
        let integrity = stage("integrity_attestation", async {
            // Public YouTube web-client key, never the server's Data API key.
            let value = http
                .json(
                    "https://jnn-pa.googleapis.com/$rpc/google.internal.waa.v1.Waa/GenerateIT",
                    &json!(["O43z0dpjhgX20SCx4KAo", snapshot]),
                    &[
                        ("Content-Type", "application/json+protobuf"),
                        ("x-goog-api-key", "AIzaSyDyT5W0Jh49F30Pqqtyfdf7pDLFKLJoAnw"),
                        ("x-user-agent", "grpc-web-javascript/0.1"),
                    ],
                )
                .await?;
            value[0]
                .as_str()
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
                .ok_or(Failure::Attestation)
        })
        .await?;
        let token = stage("po_token", async {
            session.js(&format!(r#"
                globalThis.__poToken=null;
                Promise.resolve(__signals[0](Uint8Array.from(atob({}),c=>c.charCodeAt(0))))
                  .then(mint=>mint(new TextEncoder().encode({})))
                  .then(bytes=>{{globalThis.__poToken=btoa(String.fromCharCode(...bytes)).replace(/\+/g,'-').replace(/\//g,'_');}});
            "#,argument(&integrity),argument(video_id))).await?;
            let token = session.js("__poToken").await?;
            if token == "null" || token.is_empty() { return Err(Failure::Attestation); }
            Ok(token)
        }).await?;
        let script = stage("player_script", self.player_script(http, session, &config)).await?;
        let client = mweb_context(&config);
        let mut player = stage("player_request", http.json("https://www.youtube.com/youtubei/v1/player?prettyPrint=false", &json!({
            "context":client,"videoId":video_id,"contentCheckOk":true,"racyCheckOk":true,
            "playbackContext":{"contentPlaybackContext":{"signatureTimestamp":script.timestamp,"vis":0,"splay":false,"lactMilliseconds":"-1"}}
        }), &[("X-Youtube-Client-Name","2"),("X-Youtube-Client-Version",VERSION),("X-Goog-Visitor-Id",client["client"]["visitorData"].as_str().unwrap_or_default()),("Origin","https://www.youtube.com"),("Referer","https://www.youtube.com/")])).await?;
        if player["videoDetails"]["videoId"]
            .as_str()
            .is_some_and(|id| id != video_id)
        {
            return Err(Failure::Http);
        }
        if player["playabilityStatus"]["status"] != "OK" {
            tracing::warn!("MWEB player did not return playable media");
            return Ok(player);
        }
        stage("resolve_formats", async {
            let mut skipped = 0;
            for key in ["formats","adaptiveFormats"] {
                if let Some(formats) = player["streamingData"][key].as_array_mut() {
                    let raw = std::mem::take(formats);
                    for mut format in raw {
                        match resolve_url(&format, &token, session).await {
                            Ok(url) => {
                                format["url"] = json!(url);
                                if let Some(object) = format.as_object_mut() { object.remove("signatureCipher");object.remove("cipher"); }
                                formats.push(format);
                            }
                            Err(error) => {
                                tracing::debug!(format_id = format["itag"].as_u64(), failure = ?error, "MWEB format URL transform failed");
                                skipped += 1;
                            }
                        }
                    }
                }
            }
            player["_skipped_formats"] = json!(skipped);
            tracing::info!(skipped_formats = skipped, "MWEB URLs resolved");
            Ok(())
        }).await?;
        Ok(player)
    }
    async fn player_script(
        &self,
        http: &Http,
        session: &mut Session,
        config: &Value,
    ) -> Result<PlayerScript> {
        let (id, url) = if let Some(player) = configured_player(config)? {
            player
        } else {
            let iframe = http.text("https://www.youtube.com/iframe_api").await?;
            let id = capture(r"player\\/([A-Za-z0-9_-]+)\\/", &iframe)?;
            let url =
                format!("https://www.youtube.com/s/player/{id}/player_es6.vflset/en_US/base.js");
            (id, url)
        };
        // Serialize cold preparation so concurrent workers share one analysis.
        let mut cache = self.script.lock().await;
        if let Some(cached) = cache.as_ref().filter(|cached| cached.url == url).cloned() {
            drop(cache);
            tracing::info!(cache_hit = true, player_id = %id, "Loading cached player transforms");
            session
                .js(&format!(
                    "globalThis.__decipher=new Function({})();",
                    argument(&cached.script)
                ))
                .await?;
            return Ok(cached);
        }
        tracing::info!(cache_hit = false, player_id = %id, "Fetching and parsing player script");
        let source = http.text(&url).await?;
        // Parser helpers are precompiled in the immutable startup snapshot.
        let timestamp = session
            .js(&format!("__preparePlayer({})", argument(&source)))
            .await?
            .parse()
            .map_err(|_| Failure::Runtime)?;
        let script = session.js("__playerScript").await?;
        if script.len() > 1024 * 1024 {
            return Err(Failure::Runtime);
        }
        let cached = PlayerScript {
            url,
            timestamp,
            script,
        };
        *cache = Some(cached.clone());
        tracing::info!("Player transforms cached");
        Ok(cached)
    }
}
#[derive(Debug)]
enum Failure {
    Busy,
    Http,
    Runtime,
    Attestation,
    Timeout,
}
type Result<T, E = Failure> = std::result::Result<T, E>;

struct Http {
    client: Client,
    deadline: Instant,
}
impl Http {
    fn timeout(&self) -> Result<Duration> {
        self.deadline
            .checked_duration_since(Instant::now())
            .map(|d| d.min(Duration::from_secs(30)))
            .ok_or(Failure::Timeout)
    }
    async fn text(&self, url: &str) -> Result<String> {
        let url = Url::parse(url).map_err(|_| Failure::Http)?;
        if url.scheme() != "https"
            || url.port_or_known_default() != Some(443)
            || !url.username().is_empty()
            || url.password().is_some()
            || !matches!(url.host_str(), Some("www.youtube.com" | "www.google.com"))
        {
            return Err(Failure::Http);
        }
        let response = self
            .client
            .get(url)
            .timeout(self.timeout()?)
            .send()
            .await
            .map_err(http_error)?;
        String::from_utf8(read(response).await?).map_err(|_| Failure::Http)
    }
    async fn json(&self, url: &str, body: &Value, headers: &[(&str, &str)]) -> Result<Value> {
        let mut request = self.client.post(url).timeout(self.timeout()?);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        let response = request.json(body).send().await.map_err(http_error)?;
        serde_json::from_slice(&read(response).await?).map_err(|_| Failure::Http)
    }
}
fn http_error(error: reqwest::Error) -> Failure {
    tracing::warn!(
        timeout = error.is_timeout(),
        upstream_status = error.status().map(|s| s.as_u16()),
        "MWEB upstream request failed"
    );
    if error.is_timeout() {
        Failure::Timeout
    } else {
        Failure::Http
    }
}
async fn read(response: Response) -> Result<Vec<u8>> {
    tracing::info!(
        upstream_status = response.status().as_u16(),
        "MWEB upstream responded"
    );
    let mut response = response.error_for_status().map_err(http_error)?;
    if response.content_length().is_some_and(|n| n > MAX_BYTES) {
        return Err(Failure::Http);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(http_error)? {
        if chunk.len() > (MAX_BYTES as usize).saturating_sub(bytes.len()) {
            return Err(Failure::Http);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
async fn stage<T>(name: &'static str, work: impl Future<Output = Result<T>>) -> Result<T> {
    async {
        tracing::info!("MWEB stage started");
        let started = Instant::now();
        let result = work.await;
        tracing::info!(
            elapsed_ms = started.elapsed().as_millis() as u64,
            success = result.is_ok(),
            "MWEB stage finished"
        );
        result
    }
    .instrument(tracing::info_span!("mweb.stage", stage = name))
    .await
}
fn capture(pattern: &str, text: &str) -> Result<String> {
    Regex::new(pattern)
        .expect("static expression")
        .captures(text)
        .map(|c| c[1].to_owned())
        .ok_or(Failure::Http)
}
fn homepage(html: &str) -> Result<(Value, String)> {
    let config = serde_json::from_str(&capture(r"(?s)ytcfg\.set\((\{.+?\})\);", html)?)
        .map_err(|_| Failure::Http)?;
    // The challenge is a JavaScript object literal, not necessarily JSON
    // (YouTube uses unquoted keys). Only evaluate it inside the embedded V8 context.
    let challenge = capture(r"(?s)window\.ytAtN\(\s*(\{.*?\})\s*\)", html)?;
    Ok((config, challenge))
}
fn argument(value: &str) -> String {
    serde_json::to_string(value).expect("strings serialize")
}

fn mweb_context(config: &Value) -> Value {
    let homepage_client = &config["INNERTUBE_CONTEXT"]["client"];
    // Homepage configuration belongs to desktop WEB. Carry only visitor and
    // locale information into MWEB, never desktop configInfo/experiment flags
    // that may request SABR-only metadata without downloadable format URLs.
    let mut client = json!({
        "clientName":"MWEB", "clientVersion":VERSION,
        "clientFormFactor":"SMALL_FORM_FACTOR", "platform":"MOBILE",
        "userAgent":USER_AGENT,
        "hl":homepage_client["hl"].as_str().unwrap_or("en"),
        "gl":homepage_client["gl"].as_str().unwrap_or("US"),
    });
    if let Some(visitor) = homepage_client["visitorData"].as_str() {
        client["visitorData"] = json!(visitor);
    }
    json!({"client":client})
}

fn configured_player(config: &Value) -> Result<Option<(String, String)>> {
    let Some(raw) = config["PLAYER_JS_URL"].as_str() else {
        return Ok(None);
    };
    let url = Url::parse("https://www.youtube.com/")
        .unwrap()
        .join(raw)
        .map_err(|_| Failure::Http)?;
    if url.scheme() != "https"
        || url.host_str() != Some("www.youtube.com")
        || url.port_or_known_default() != Some(443)
        || !url.username().is_empty()
        || url.password().is_some()
        || !url.path().ends_with("/base.js")
    {
        return Err(Failure::Http);
    }
    let id = capture(r"^/s/player/([A-Za-z0-9_-]+)/", url.path())?;
    // Match the homepage revision while retaining the lightweight ES6 variant
    // used by the MWEB decoder. Desktop IAS code has different extraction
    // patterns even when it shares that revision and signature timestamp.
    let script = format!("https://www.youtube.com/s/player/{id}/player_es6.vflset/en_US/base.js");
    Ok(Some((id, script)))
}

async fn resolve_url(format: &Value, token: &str, context: &mut Session) -> Result<String> {
    let params: BTreeMap<String, String> = if let Some(cipher) = format["signatureCipher"]
        .as_str()
        .or_else(|| format["cipher"].as_str())
    {
        Url::parse(&format!("https://cipher.invalid/?{cipher}"))
            .map_err(|_| Failure::Runtime)?
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect()
    } else {
        BTreeMap::new()
    };
    let raw = format["url"]
        .as_str()
        .or_else(|| params.get("url").map(String::as_str))
        .ok_or_else(|| {
            tracing::debug!("Player returned format metadata without a media URL");
            Failure::Runtime
        })?;
    let mut url = Url::parse(raw).map_err(|_| Failure::Runtime)?;
    if url.scheme() != "https"
        || !url
            .host_str()
            .is_some_and(|host| host.ends_with(".googlevideo.com"))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port_or_known_default() != Some(443)
    {
        return Err(Failure::Runtime);
    }
    let n = url
        .query_pairs()
        .find(|(k, _)| k == "n")
        .map(|(_, v)| v.into_owned());
    let s = params.get("s");
    let sp = params.get("sp").map(String::as_str).unwrap_or("signature");
    let mut replace = BTreeMap::from([
        ("pot".to_owned(), token.to_owned()),
        ("cver".to_owned(), VERSION.into()),
    ]);
    if n.is_some() || s.is_some() {
        tracing::debug!(
            has_n = n.is_some(),
            has_encrypted_signature = s.is_some(),
            "Decoding player URL parameters"
        );
        let output = context
            .js(&format!(
                "JSON.stringify(__decipher({},{},{}))",
                argument(n.as_deref().unwrap_or_default()),
                argument(sp),
                argument(s.map(String::as_str).unwrap_or_default())
            ))
            .await?;
        let output: Value = serde_json::from_str(&output).map_err(|_| Failure::Runtime)?;
        if n.is_some() {
            let value = output["n"]
                .as_str()
                .filter(|v| !v.is_empty() && !v.starts_with("enhanced_except_"))
                .ok_or_else(|| {
                    tracing::debug!(
                        exception_fallback = output["n"]
                            .as_str()
                            .is_some_and(|n| n.starts_with("enhanced_except_")),
                        "Player did not decode the n parameter"
                    );
                    Failure::Runtime
                })?;
            replace.insert("n".into(), value.into());
        }
        if s.is_some() {
            let value = output["signature"]
                .as_str()
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                    tracing::debug!("Player did not decode the signature parameter");
                    Failure::Runtime
                })?;
            replace.insert(sp.into(), value.into());
        }
    } else if let Some(signature) = params.get("sig").or_else(|| params.get("signature")) {
        replace.insert(sp.into(), signature.clone());
    }
    let pairs: Vec<_> = url
        .query_pairs()
        .filter(|(key, _)| !replace.contains_key(key.as_ref()))
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    url.set_query(None);
    url.query_pairs_mut()
        .extend_pairs(pairs)
        .extend_pairs(replace);
    url.set_fragment(None);
    Ok(url.into())
}

#[cfg(test)]
mod tests;
