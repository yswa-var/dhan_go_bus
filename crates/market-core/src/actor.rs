use std::sync::Arc;

use anyhow::{Result, ensure};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::{GoldMarketState, MarketEvent, MarketStateConfig, MarketView};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ActorStats {
    pub events_applied: u64,
    pub snapshots_published: u64,
}

pub struct MarketStateActor {
    pub events: mpsc::Sender<MarketEvent>,
    pub snapshots: mpsc::Receiver<Arc<MarketView>>,
    pub task: JoinHandle<Result<ActorStats>>,
}

pub fn spawn_market_state_actor(
    config: MarketStateConfig,
    event_capacity: usize,
    snapshot_capacity: usize,
) -> Result<MarketStateActor> {
    ensure!(
        event_capacity > 0,
        "event channel capacity must be positive"
    );
    ensure!(
        snapshot_capacity > 0,
        "snapshot channel capacity must be positive"
    );
    let mut state = GoldMarketState::new(config)?;
    let (event_tx, mut event_rx) = mpsc::channel::<MarketEvent>(event_capacity);
    let (snapshot_tx, snapshot_rx) = mpsc::channel(snapshot_capacity);
    let task = tokio::spawn(async move {
        let mut stats = ActorStats::default();
        while let Some(event) = event_rx.recv().await {
            let snapshot = state.apply(&event)?;
            stats.events_applied = stats.events_applied.saturating_add(1);
            snapshot_tx
                .send(snapshot)
                .await
                .map_err(|_| anyhow::anyhow!("market-view receiver closed"))?;
            stats.snapshots_published = stats.snapshots_published.saturating_add(1);
        }
        Ok(stats)
    });
    Ok(MarketStateActor {
        events: event_tx,
        snapshots: snapshot_rx,
        task,
    })
}
