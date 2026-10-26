use std::{env, error::Error};
use yic::{YouTubeClient, app, init_tracing};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_tracing()?;
    let api_key = env::var("YOUTUBE_API_KEY")
        .ok()
        .filter(|key| !key.trim().is_empty())
        .ok_or("YOUTUBE_API_KEY must be set to a YouTube Data API v3 key")?;
    let youtube = YouTubeClient::new(api_key)?;
    let address = env::var("BIND_ADDRESS").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let listener = tokio::net::TcpListener::bind(&address).await?;
    log::info!("Listening on http://{}", listener.local_addr()?);
    axum::serve(listener, app(youtube))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            log::info!("Shutdown requested; waiting for active requests");
        })
        .await?;
    log::info!("Server stopped");
    Ok(())
}
