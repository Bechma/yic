use super::*;

#[test]
fn selects_homepage_revision_in_the_mweb_compatible_player_variant() {
    let config = json!({"PLAYER_JS_URL":"/s/player/386a813f/player_ias.vflset/es_ES/base.js"});
    let (id, url) = configured_player(&config).unwrap().unwrap();
    assert_eq!(id, "386a813f");
    assert_eq!(
        url,
        "https://www.youtube.com/s/player/386a813f/player_es6.vflset/en_US/base.js"
    );
    assert!(configured_player(&json!({})).unwrap().is_none());
    for raw in [
        "http://www.youtube.com/s/player/id/base.js",
        "https://evil.example/s/player/id/base.js",
        "/api/timedtext",
        "https://user@www.youtube.com/s/player/id/base.js",
    ] {
        assert!(configured_player(&json!({"PLAYER_JS_URL":raw})).is_err());
    }
}

#[test]
fn mweb_context_preserves_visitor_binding_without_desktop_experiments() {
    let config = json!({"INNERTUBE_CONTEXT":{
        "client":{"clientName":"WEB","clientVersion":"desktop","visitorData":"visitor","hl":"es","gl":"ES","configInfo":{"appInstallData":"desktop"},"rolloutToken":"desktop"},
        "request":{"internalExperimentFlags":["desktop"]}
    }});
    let context = mweb_context(&config);
    assert_eq!(context["client"]["clientName"], "MWEB");
    assert_eq!(context["client"]["clientVersion"], VERSION);
    assert_eq!(context["client"]["visitorData"], "visitor");
    assert_eq!(context["client"]["hl"], "es");
    assert_eq!(context["client"]["gl"], "ES");
    assert!(context["client"].get("configInfo").is_none());
    assert!(context["client"].get("rolloutToken").is_none());
    assert!(context.get("request").is_none());
}

#[tokio::test]
async fn homepage_challenge_accepts_javascript_object_literals() {
    let (config,challenge) = homepage(r#"ytcfg.set({"INNERTUBE_CONTEXT":{}});window.ytAtN({R:'{"bgChallenge":{"name":"test"}}'});"#).unwrap();
    assert!(config["INNERTUBE_CONTEXT"].is_object());
    let pool = EmbeddedPool::default();
    let mut session = pool.acquire().await.unwrap();
    session.reset().await.unwrap();
    assert_eq!(
        session
            .js(&format!("JSON.parse(({challenge}).R).bgChallenge.name"))
            .await
            .unwrap(),
        "test"
    );
    session.recycle().await.unwrap();
}

#[tokio::test]
async fn embedded_runtime_evaluates_repeated_nested_code_with_local_eval_bindings() {
    let pool = EmbeddedPool::default();
    let mut session = pool.acquire().await.unwrap();
    session.reset().await.unwrap();
    let program = r#"(function(){var outer=42; var parent=eval('0,function(q){return function(q){return eval(q)}(q)}');parent('var eval="shadow";eval');return parent('outer')})()"#;
    assert_eq!(session.js(program).await.unwrap(), "42");
    session.recycle().await.unwrap();
}

#[tokio::test]
async fn media_urls_decode_cipher_and_n_and_bind_token_without_losing_signed_parameters() {
    let pool = EmbeddedPool::default();
    let mut session = pool.acquire().await.unwrap();
    session.reset().await.unwrap();
    session
        .js("globalThis.__decipher=(n,sp,s)=>({n:'decoded-n',signature:'decoded-signature'});")
        .await
        .unwrap();
    let mut cipher = Url::parse("https://cipher.invalid/").unwrap();
    cipher
        .query_pairs_mut()
        .append_pair(
            "url",
            "https://rr1.googlevideo.com/videoplayback?n=encrypted-n&sig=old&x=a%2Bb%2Fc",
        )
        .append_pair("s", "encrypted-signature")
        .append_pair("sp", "sig");
    let url = resolve_url(
        &json!({"signatureCipher":cipher.query().unwrap()}),
        "video-bound-token",
        &mut session,
    )
    .await
    .unwrap();
    let url = Url::parse(&url).unwrap();
    let pairs: BTreeMap<_, _> = url.query_pairs().collect();
    assert_eq!(pairs["n"], "decoded-n");
    assert_eq!(pairs["sig"], "decoded-signature");
    assert_eq!(pairs["pot"], "video-bound-token");
    assert_eq!(pairs["x"], "a+b/c");
    assert_eq!(pairs["cver"], VERSION);
    for url in [
        "http://localhost/private",
        "https://googlevideo.com.evil.example/private",
    ] {
        assert!(
            resolve_url(&json!({"url":url}), "token", &mut session)
                .await
                .is_err()
        );
    }
    session
        .js("__decipher=()=>({n:'enhanced_except_failure'});")
        .await
        .unwrap();
    assert!(
        resolve_url(
            &json!({"url":"https://rr1.googlevideo.com/videoplayback?n=encrypted"}),
            "token",
            &mut session
        )
        .await
        .is_err()
    );
    session.recycle().await.unwrap();
}

#[tokio::test]
async fn vendored_player_extractor_runs_in_embedded_v8_and_executes_selected_dependencies() {
    let pool = EmbeddedPool::default();
    let mut session = pool.acquire().await.unwrap();
    session.reset().await.unwrap();
    let source = r#"(function(g){g.U=class{constructor(url){this.query={}}set(k,v){this.query[k]=v}get(k){return this.query[k]}decode(){this.set('n','decoded');this.set('signature','signature-result')}};var build=function(url,sp='',s=''){url=new g.U(url);url.set('alr','yes');return url};var info=function(){return{signatureTimestamp:12345}};})({});"#;
    assert_eq!(
        session
            .js(&format!("__preparePlayer({})", argument(source)))
            .await
            .unwrap(),
        "12345"
    );
    let output = session
        .js("JSON.stringify(__decipher('original','signature','encrypted'))")
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&output).unwrap(),
        json!({"n":"decoded","signature":"signature-result"})
    );
    session.recycle().await.unwrap();
}

#[tokio::test]
#[ignore = "live YouTube integration"]
async fn native_embedded_probe() {
    let extractor = MwebExtractor::default();
    let player = extractor.extract("P9iH4Wadwv4").await.unwrap();
    assert_eq!(player["playabilityStatus"]["status"], "OK");
    let formats = player["streamingData"]["adaptiveFormats"]
        .as_array()
        .unwrap();
    let selected = formats
        .iter()
        .find(|format| format["itag"] == 140 && format["audioTrack"]["id"] == "es-US.4")
        .unwrap();
    let url = Url::parse(selected["url"].as_str().unwrap()).unwrap();
    assert!(
        url.query_pairs()
            .any(|(key, value)| key == "pot" && !value.is_empty())
    );
    println!(
        "MWEB formats={} selected_length={}",
        formats.len(),
        selected["contentLength"]
    );
}
