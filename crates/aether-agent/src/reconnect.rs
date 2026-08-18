//! Staying attached to a controller that went away.
//!
//! An agent used to exit when its connection dropped, which meant a controller
//! restart cost you every node in the mesh and whatever they were holding.
//! Keeping the process alive keeps the data alive with it: the store is in
//! memory, and on reconnecting the node tells the new controller what it has.

use std::time::Duration;

use tracing::{info, warn};

/// First gap between attempts. Doubles from here.
pub const FIRST_DELAY: Duration = Duration::from_secs(1);

/// Runs `attempt` until it is asked to stop, reconnecting in between.
///
/// The gap doubles from a second up to `ceiling`: a controller that is down
/// for an hour should not be asked about it three thousand times, and one that
/// restarts in five seconds should not be waited on for a minute.
///
/// `ceiling` of zero means do not reconnect at all, which is what the agent
/// did before this existed and is still the right thing for a one-shot.
pub async fn with_reconnect(
    ceiling: Duration,
    mut attempt: impl AsyncFnMut() -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    let mut delay = FIRST_DELAY;
    loop {
        match attempt().await {
            Ok(()) => info!("the controller closed the connection"),
            // Without a ceiling there is nothing to do with the error but
            // report it, so it goes back to the caller intact.
            Err(error) if ceiling.is_zero() => return Err(error),
            Err(error) => warn!(%error, "lost the controller"),
        }
        if ceiling.is_zero() {
            return Ok(());
        }

        info!(secs = delay.as_secs(), "reconnecting");
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(ceiling);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test]
    async fn without_a_ceiling_the_first_failure_is_returned() {
        let attempts = AtomicUsize::new(0);

        let outcome = with_reconnect(Duration::ZERO, async || {
            attempts.fetch_add(1, Ordering::Relaxed);
            Err(anyhow::anyhow!("refused"))
        })
        .await;

        assert!(outcome.is_err());
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn without_a_ceiling_a_closed_connection_ends_the_run() {
        let attempts = AtomicUsize::new(0);

        with_reconnect(Duration::ZERO, async || {
            attempts.fetch_add(1, Ordering::Relaxed);
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(attempts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_failing_connection_is_tried_again() {
        let attempts = AtomicUsize::new(0);

        // Runs forever by design, so the assertion is what stops it.
        let _ = tokio::time::timeout(
            Duration::from_secs(120),
            with_reconnect(Duration::from_secs(30), async || {
                attempts.fetch_add(1, Ordering::Relaxed);
                Err(anyhow::anyhow!("connection refused"))
            }),
        )
        .await;

        // 1s, 2s, 4s, 8s, 16s, then 30s each: eight attempts inside two
        // minutes. The point is that it backs off rather than spinning.
        assert_eq!(attempts.load(Ordering::Relaxed), 8);
    }

    #[tokio::test(start_paused = true)]
    async fn the_gap_never_grows_past_the_ceiling() {
        let attempts = AtomicUsize::new(0);

        let _ = tokio::time::timeout(
            Duration::from_secs(20),
            with_reconnect(Duration::from_secs(2), async || {
                attempts.fetch_add(1, Ordering::Relaxed);
                Err(anyhow::anyhow!("connection refused"))
            }),
        )
        .await;

        // 1s then 2s forever: eleven attempts in twenty seconds, not five.
        assert_eq!(attempts.load(Ordering::Relaxed), 11);
    }
}
