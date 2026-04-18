//! Shared runtime helpers for pipeline execution.
//!
//! All runnable pipeline types share the same shape: spawn stages, spawn an
//! error-rail sink, join every handle, surface the first fatal error without
//! cancelling sibling tasks.

use crate::error::{Result, StreamSafeError};
use crate::sink::Sink;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Spawn the error-rail sink task that drains `error_rx`.
///
/// The task exits naturally when every [`mpsc::Sender`] for the error rail has
/// been dropped (and the channel closes).
pub(crate) fn spawn_error_sink<E>(
    mut sink: E,
    mut error_rx: mpsc::Receiver<StreamSafeError>,
    token: CancellationToken,
    handles: &mut Vec<JoinHandle<Result<()>>>,
) where
    E: Sink<Input = StreamSafeError>,
{
    handles.push(tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = token.cancelled() => return Ok(()),
                item = error_rx.recv() => {
                    match item {
                        Some(err) => sink.consume(err).await?,
                        None => return sink.finish().await,
                    }
                }
            }
        }
    }));
}

/// Await every handle, surfacing the first fatal error.
///
/// A failure in any task does not cancel siblings — they run to their natural
/// end. Only external cancellation (via the pipeline's token) aborts early.
pub(crate) async fn join_handles(handles: Vec<JoinHandle<Result<()>>>) -> Result<()> {
    let mut first_error: Option<StreamSafeError> = None;
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if first_error.is_none() {
                    first_error = Some(e);
                }
            }
            Err(join_err) => {
                if first_error.is_none() {
                    first_error = Some(StreamSafeError::other(join_err));
                }
            }
        }
    }
    first_error.map_or(Ok(()), Err)
}

/// Build an internal token that cancels on Ctrl-C. Used by `.run()` methods
/// when no external token is provided.
pub(crate) fn ctrlc_token() -> CancellationToken {
    let token = CancellationToken::new();
    let shutdown = token.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        shutdown.cancel();
    });
    token
}
