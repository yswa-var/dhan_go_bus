use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, ValueEnum};
use goldlab_replay::{RecordingReader, ReplayEngine, ReplaySpeed};

#[derive(Clone, Copy, Debug, ValueEnum)]
enum SpeedArg {
    Max,
    WallClock,
}

impl From<SpeedArg> for ReplaySpeed {
    fn from(value: SpeedArg) -> Self {
        match value {
            SpeedArg::Max => Self::Max,
            SpeedArg::WallClock => Self::WallClock,
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "Deterministically replay a GOLDLAB Parquet recording")]
struct Args {
    #[arg(long)]
    input: PathBuf,
    #[arg(long, value_enum, default_value_t = SpeedArg::Max)]
    speed: SpeedArg,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long, default_value_t = 1024)]
    batch_records: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let summary = tokio::task::spawn_blocking(move || run(args))
        .await
        .context("join replay worker")??;
    serde_json::to_writer_pretty(io::stdout().lock(), &summary).context("write replay summary")?;
    println!();
    Ok(())
}

fn run(args: Args) -> Result<goldlab_replay::ReplaySummary> {
    let reader = RecordingReader::open(&args.input, args.batch_records)?;
    let speed = ReplaySpeed::from(args.speed);
    match args.output {
        Some(path) => {
            let file = File::create(&path)
                .with_context(|| format!("create replay output {}", path.display()))?;
            let mut writer = BufWriter::new(file);
            let summary = ReplayEngine::new(speed).run(reader, &mut writer)?;
            writer.flush().context("flush replay output file")?;
            writer
                .get_ref()
                .sync_all()
                .context("sync replay output file")?;
            Ok(summary)
        }
        None => ReplayEngine::new(speed).run(reader, io::sink()),
    }
}
