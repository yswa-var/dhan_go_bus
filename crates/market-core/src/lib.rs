mod event;

pub use event::{MarketEvent, RecordedEventFields, SourceCoordinate};

pub const PARQUET_SCHEMA_VERSION: u16 = 1;
