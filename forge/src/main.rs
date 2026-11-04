//! The `forge` binary: load the config, open the registry and the change
//! log, start the webhook dispatcher and the alert engine, serve the API
//! until Ctrl-C or SIGTERM.
//!
//! ```text
//!   config ─► tracing ─► Registry::open (takes the lock) ─► CheckpointStore + ChangelogReader::open
//!     ─► LensClient ─► WebhookService::start ─► AlertEngine (handle taken, run spawned)
//!     ─► release_final(committed Final slot) ─► api::serve
//!                                                   │ Ctrl-C / SIGTERM, or the engine stops
//!                                                   ▼
//!       drop the registry (releases the lock) ◄── await both ◄── cancel the engine, stop the dispatcher
//! ```
//!
//! Every variable is read from the environment; see `forge::config`. A
//! config error is reported on stderr before tracing is set up, since the
//! log level is itself part of the config.

use std::io::IsTerminal;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;

use forge::{
    AlertEngine, AppState, ChangelogReader, CheckpointStore, ForgeConfig, LensApi, LensClient, Registry,
    WebhookService,
};
use tokio_util::sync::CancellationToken;
use tracing::Level;

#[tokio::main]
async fn main() -> ExitCode {
    let config = match ForgeConfig::load() {
        Ok(config) => Arc::new(config),
        Err(err) => {
            eprintln!("forge: {err}");
            return ExitCode::FAILURE;
        }
    };
    let level = Level::from_str(config.log_level()).expect("log_level is validated when the config is loaded");
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_target(false)
        .with_ansi(std::io::stdout().is_terminal())
        .init();
    config.display();

    match run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(kind = err.kind(), error = %err, "FORGE failed");
            ExitCode::FAILURE
        }
    }
}

async fn run(config: Arc<ForgeConfig>) -> forge::ForgeResult<()> {
    // Held until the end of this function: dropping it releases the lock.
    let registry = Arc::new(Registry::open(&config)?);
    let reader = ChangelogReader::open(&config, CheckpointStore::for_config(&config))?;
    let committed_final = reader.highest_final_slot();
    let lens: Arc<dyn LensApi> = Arc::new(LensClient::new(&config)?);
    let webhook = Arc::new(WebhookService::start(&config, registry.db().clone())?);

    let engine = AlertEngine::new(registry.clone(), reader, lens.clone(), webhook.clone(), &config)?;
    let handle = engine.handle();
    let cancel = CancellationToken::new();
    let engine_task = tokio::spawn({
        let cancel = cancel.clone();
        async move { engine.run(cancel).await }
    });

    // Deliveries held for slots the committed change log already finalized,
    // in case the last run stopped between the two.
    if committed_final > 0 {
        match webhook.release_final(committed_final) {
            Ok(released) => tracing::info!(final_slot = committed_final, released, "released held deliveries"),
            Err(err) => tracing::warn!(final_slot = committed_final, error = %err, "cannot release held deliveries"),
        }
    }

    let state = AppState::new(registry.clone(), lens, webhook.clone(), handle, config);
    // Stop serving on a signal, and also if the engine stops on its own: a
    // FORGE that answers requests but evaluates no alerts would look healthy.
    let stop = {
        let cancel = cancel.clone();
        async move {
            tokio::select! {
                () = forge::shutdown_signal() => {}
                () = cancel.cancelled() => {}
            }
        }
    };
    let engine_watch = tokio::spawn({
        let cancel = cancel.clone();
        async move {
            let result = engine_task.await;
            cancel.cancel();
            result
        }
    });
    let served = forge::serve(state, stop).await;

    cancel.cancel();
    webhook.shutdown().await;
    match engine_watch.await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(error = %err, "alert engine task panicked"),
        Err(err) => tracing::error!(error = %err, "alert engine watcher panicked"),
    }
    drop(webhook);
    drop(registry);
    tracing::info!("FORGE stopped");
    served
}
