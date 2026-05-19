//! Coordinator for paused agent runs.
//!
//! When the agent ends a turn with `CATEGORY_CONFIRMATION_NEEDED: <slug>`, the runtime
//! parks the loop on a `tokio::oneshot` channel keyed by `run_id`. A separate HTTP
//! endpoint (POST /api/v1/agent/runs/:run_id/continue) looks the run up via this coordinator
//! and pushes the user's reply into the channel, resuming the same loop.
//!
//! This keeps a category question as **one run** instead of two with a synthetic chat
//! message between them.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryAssignment {
    pub merchant_signature_id: String,
    pub included: bool,
}

/// The structured reply the UI sends back when the user clicks Apply on a category card.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunContinuation {
    pub category_slug: String,
    pub assignments: Vec<CategoryAssignment>,
}

/// Errors a continue-request can hit.
#[derive(Debug)]
pub enum ContinueError {
    NotFound(String),
    AlreadyResumed(String),
}

impl std::fmt::Display for ContinueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(id) => write!(
                f,
                "no paused run found with id '{id}' — it may have completed, been cancelled, or timed out"
            ),
            Self::AlreadyResumed(id) => write!(f, "paused run '{id}' was already resumed"),
        }
    }
}

impl std::error::Error for ContinueError {}

#[derive(Default)]
pub struct RunCoordinator {
    parked: Mutex<HashMap<String, oneshot::Sender<RunContinuation>>>,
}

impl RunCoordinator {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Park a run: register a receiver under `run_id` and return the awaiter.
    /// The runtime awaits on the returned `Receiver`. If the channel is dropped before
    /// the user replies (client disconnect, server shutdown, timeout), the await returns
    /// `Err(RecvError)` and the runtime should treat the run as abandoned.
    pub async fn park(&self, run_id: &str) -> oneshot::Receiver<RunContinuation> {
        let (tx, rx) = oneshot::channel();
        let mut g = self.parked.lock().await;
        // Overwrite any stale entry — if a previous park was abandoned, the old sender is
        // dropped here, the old receiver returns an error, and the runner cleans up.
        g.insert(run_id.to_string(), tx);
        rx
    }

    /// Resume a parked run. Returns `Ok(())` if a parked run was found and woken,
    /// `Err(NotFound)` if no run is parked under that id, `Err(AlreadyResumed)` if the
    /// sender was somehow already consumed (defensive — shouldn't happen with the API).
    pub async fn resume(
        &self,
        run_id: &str,
        cont: RunContinuation,
    ) -> Result<(), ContinueError> {
        let mut g = self.parked.lock().await;
        let Some(tx) = g.remove(run_id) else {
            return Err(ContinueError::NotFound(run_id.to_string()));
        };
        tx.send(cont)
            .map_err(|_| ContinueError::AlreadyResumed(run_id.to_string()))?;
        Ok(())
    }

    /// Drop a parked entry without resuming (used by the runtime's timeout / cancellation
    /// path to clean up after itself).
    pub async fn cancel(&self, run_id: &str) {
        let mut g = self.parked.lock().await;
        g.remove(run_id);
    }

    /// Diagnostic: how many runs are currently parked.
    pub async fn parked_count(&self) -> usize {
        self.parked.lock().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn park_then_resume_delivers_continuation() {
        let coord = RunCoordinator::new();
        let rx = coord.park("run-1").await;
        let cont = RunContinuation {
            category_slug: "groceries".into(),
            assignments: vec![CategoryAssignment {
                merchant_signature_id: "m1".into(),
                included: true,
            }],
        };
        coord.resume("run-1", cont.clone()).await.expect("resume");
        let got = rx.await.expect("ok");
        assert_eq!(got.category_slug, "groceries");
        assert_eq!(got.assignments.len(), 1);
        assert_eq!(coord.parked_count().await, 0);
    }

    #[tokio::test]
    async fn resume_unknown_run_returns_not_found() {
        let coord = RunCoordinator::new();
        let err = coord
            .resume(
                "no-such-run",
                RunContinuation {
                    category_slug: "x".into(),
                    assignments: vec![],
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ContinueError::NotFound(_)));
    }

    #[tokio::test]
    async fn cancel_drops_the_parked_entry_and_receiver_errors() {
        let coord = RunCoordinator::new();
        let rx = coord.park("run-cancel").await;
        coord.cancel("run-cancel").await;
        assert_eq!(coord.parked_count().await, 0);
        assert!(rx.await.is_err()); // sender dropped
    }

    #[tokio::test]
    async fn re_parking_same_id_dropps_old_receiver() {
        let coord = RunCoordinator::new();
        let rx_old = coord.park("dup").await;
        let rx_new = coord.park("dup").await;
        // The old receiver should error (its sender was overwritten + dropped).
        assert!(rx_old.await.is_err());
        // The new receiver is the live one.
        let cont = RunContinuation {
            category_slug: "x".into(),
            assignments: vec![],
        };
        coord.resume("dup", cont).await.expect("resume");
        assert!(rx_new.await.is_ok());
    }
}
