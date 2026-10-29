use std::time::{Duration, Instant};

use crate::record::SourceRecord;

#[derive(Clone, Copy, Debug)]
pub struct BatchLimits {
    pub max_records: usize,
    pub max_bytes: usize,
    pub max_age: Duration,
}

impl BatchLimits {
    pub fn validate(self) -> anyhow::Result<Self> {
        anyhow::ensure!(
            self.max_records > 0,
            "max_records must be greater than zero"
        );
        anyhow::ensure!(self.max_bytes > 0, "max_bytes must be greater than zero");
        anyhow::ensure!(!self.max_age.is_zero(), "max_age must be greater than zero");
        Ok(self)
    }
}

pub struct BatchBuffer {
    limits: BatchLimits,
    records: Vec<SourceRecord>,
    bytes: usize,
    started_at: Option<Instant>,
}

impl BatchBuffer {
    #[must_use]
    pub fn new(limits: BatchLimits) -> Self {
        Self {
            limits,
            records: Vec::with_capacity(limits.max_records),
            bytes: 0,
            started_at: None,
        }
    }

    pub fn push(&mut self, record: SourceRecord) {
        if self.started_at.is_none() {
            self.started_at = Some(Instant::now());
        }
        self.bytes = self.bytes.saturating_add(record.approximate_size());
        self.records.push(record);
    }

    #[must_use]
    pub fn should_rotate(&self) -> bool {
        self.records.len() >= self.limits.max_records || self.bytes >= self.limits.max_bytes
    }

    #[must_use]
    pub fn age_expired(&self) -> bool {
        self.started_at
            .is_some_and(|started| started.elapsed() >= self.limits.max_age)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn take(&mut self) -> Vec<SourceRecord> {
        self.bytes = 0;
        self.started_at = None;
        std::mem::replace(
            &mut self.records,
            Vec::with_capacity(self.limits.max_records),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn age_expiry_uses_batch_start() {
        let limits = BatchLimits {
            max_records: 10,
            max_bytes: 1024,
            max_age: Duration::from_secs(1),
        };
        let mut batch = BatchBuffer::new(limits);
        batch.started_at = Instant::now().checked_sub(Duration::from_secs(2));
        assert!(batch.age_expired());
    }
}
