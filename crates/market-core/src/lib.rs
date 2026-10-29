mod actor;
mod event;
mod metadata;
mod packet;
mod state;

pub use actor::{ActorStats, MarketStateActor, spawn_market_state_actor};
pub use event::{MarketEvent, RecordedEventFields, SourceCoordinate};
pub use metadata::{InstrumentKind, InstrumentMetadata, load_resolved_contracts};
pub use packet::{DepthLevel, FiveLevelDepth, LatestQuote};
pub use state::{
    Freshness, GoldMarketState, IncompleteReason, InstrumentView, MarketStateConfig, MarketView,
    SourcePartition,
};

pub const PARQUET_SCHEMA_VERSION: u16 = 1;
pub const MARKET_VIEW_SCHEMA_VERSION: u16 = 1;
