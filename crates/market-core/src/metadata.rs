use std::collections::BTreeSet;
use std::io::Read;

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InstrumentKind {
    Future,
    Call,
    Put,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct InstrumentMetadata {
    pub security_id: String,
    pub underlying: String,
    pub symbol: String,
    pub kind: InstrumentKind,
    pub exchange_segment: String,
    pub expiry: String,
    pub strike: Option<String>,
}

#[derive(Deserialize)]
struct ResolvedContractRow {
    #[serde(rename = "SECURITY_ID")]
    security_id: String,
    #[serde(rename = "UNDERLYING")]
    underlying: String,
    #[serde(rename = "SYMBOL")]
    symbol: String,
    #[serde(rename = "INSTRUMENT")]
    instrument: String,
    #[serde(rename = "EXCHANGE_SEGMENT")]
    exchange_segment: String,
    #[serde(rename = "EXPIRY")]
    expiry: String,
    #[serde(rename = "STRIKE")]
    strike: String,
    #[serde(rename = "OPTION_TYPE")]
    option_type: String,
}

pub fn load_resolved_contracts(reader: impl Read) -> Result<Vec<InstrumentMetadata>> {
    let mut csv = csv::Reader::from_reader(reader);
    let mut instruments = Vec::new();
    let mut security_ids = BTreeSet::new();
    for (index, row) in csv.deserialize::<ResolvedContractRow>().enumerate() {
        let row = row.with_context(|| format!("decode resolved-contract row {}", index + 2))?;
        ensure!(
            !row.security_id.is_empty()
                && row.security_id.bytes().all(|byte| byte.is_ascii_digit()),
            "resolved-contract row {} has invalid security ID",
            index + 2
        );
        ensure!(
            security_ids.insert(row.security_id.clone()),
            "duplicate resolved-contract security ID {}",
            row.security_id
        );
        ensure!(
            row.exchange_segment == "MCX_COMM",
            "security {} is not an MCX_COMM contract",
            row.security_id
        );
        ensure!(
            row.underlying.starts_with("GOLD"),
            "security {} is not a GOLD-family contract",
            row.security_id
        );
        let (kind, strike) = match (row.instrument.as_str(), row.option_type.as_str()) {
            ("FUTCOM", "XX") => (InstrumentKind::Future, None),
            ("OPTFUT", "CE") => (InstrumentKind::Call, normalized_strike(&row.strike)?),
            ("OPTFUT", "PE") => (InstrumentKind::Put, normalized_strike(&row.strike)?),
            _ => bail!(
                "security {} has unsupported instrument/option type {}/{}",
                row.security_id,
                row.instrument,
                row.option_type
            ),
        };
        instruments.push(InstrumentMetadata {
            security_id: row.security_id,
            underlying: row.underlying,
            symbol: row.symbol,
            kind,
            exchange_segment: row.exchange_segment,
            expiry: row.expiry,
            strike,
        });
    }
    ensure!(!instruments.is_empty(), "resolved-contract CSV is empty");
    instruments.sort_by(|left, right| left.security_id.cmp(&right.security_id));
    Ok(instruments)
}

fn normalized_strike(value: &str) -> Result<Option<String>> {
    let parsed: f64 = value
        .parse()
        .with_context(|| format!("parse option strike {value:?}"))?;
    ensure!(
        parsed.is_finite() && parsed > 0.0,
        "invalid option strike {value:?}"
    );
    Ok(Some(value.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_future_call_and_put_without_guessing_security_ids() -> Result<()> {
        let csv = concat!(
            "SECURITY_ID,UNDERLYING,SYMBOL,INSTRUMENT,EXCHANGE_SEGMENT,EXPIRY,STRIKE,OPTION_TYPE\n",
            "3,GOLD,GOLD FUT,FUTCOM,MCX_COMM,2026-11-01,0.00000,XX\n",
            "1,GOLDM,GOLDM CALL,OPTFUT,MCX_COMM,2026-10-29,140000.00000,CE\n",
            "2,GOLDM,GOLDM PUT,OPTFUT,MCX_COMM,2026-10-29,140000.00000,PE\n"
        );
        let instruments = load_resolved_contracts(csv.as_bytes())?;
        assert_eq!(instruments.len(), 3);
        assert_eq!(instruments[0].kind, InstrumentKind::Call);
        assert_eq!(instruments[1].kind, InstrumentKind::Put);
        assert_eq!(instruments[2].kind, InstrumentKind::Future);
        assert_eq!(instruments[0].strike.as_deref(), Some("140000.00000"));
        assert_eq!(instruments[2].strike, None);
        Ok(())
    }
}
