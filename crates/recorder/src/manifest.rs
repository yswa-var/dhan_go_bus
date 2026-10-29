use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::record::{PARQUET_SCHEMA_VERSION, SourceCoordinate};

pub const MANIFEST_SCHEMA_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OffsetRange {
    pub topic: String,
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
}

impl OffsetRange {
    #[must_use]
    pub fn contains(&self, coordinate: &SourceCoordinate) -> bool {
        self.topic == coordinate.topic
            && self.partition == coordinate.partition
            && (self.first_offset..=self.last_offset).contains(&coordinate.offset)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FileEntry {
    pub path: String,
    pub row_count: usize,
    pub sha256: String,
    pub created_at: String,
    pub recorder_version: String,
    pub code_revision: String,
    pub configuration_json: String,
    pub fixture_provenance: Option<String>,
    pub ranges: Vec<OffsetRange>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub manifest_schema_version: u16,
    pub parquet_schema_version: u16,
    pub files: Vec<FileEntry>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            manifest_schema_version: MANIFEST_SCHEMA_VERSION,
            parquet_schema_version: PARQUET_SCHEMA_VERSION,
            files: Vec::new(),
        }
    }
}

impl Manifest {
    pub fn load(root: &Path) -> Result<Self> {
        let path = root.join("manifest.json");
        if !path.exists() {
            return Ok(Self::default());
        }
        let data = fs::read(&path).with_context(|| format!("read {}", path.display()))?;
        let manifest: Self =
            serde_json::from_slice(&data).with_context(|| format!("decode {}", path.display()))?;
        if manifest.manifest_schema_version != MANIFEST_SCHEMA_VERSION
            || manifest.parquet_schema_version != PARQUET_SCHEMA_VERSION
        {
            bail!("unsupported manifest or Parquet schema version");
        }
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<()> {
        let mut paths = BTreeSet::new();
        for entry in &self.files {
            if entry.row_count == 0 || entry.ranges.is_empty() {
                bail!("manifest entry {} is empty", entry.path);
            }
            if !paths.insert(&entry.path) {
                bail!("manifest contains duplicate path {}", entry.path);
            }
            for range in &entry.ranges {
                if range.first_offset < 0 || range.last_offset < range.first_offset {
                    bail!("manifest contains invalid offset range in {}", entry.path);
                }
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn contains(&self, coordinate: &SourceCoordinate) -> bool {
        self.files
            .iter()
            .flat_map(|entry| &entry.ranges)
            .any(|range| range.contains(coordinate))
    }

    #[must_use]
    pub fn contains_path(&self, path: &str) -> bool {
        self.files.iter().any(|entry| entry.path == path)
    }

    pub fn add(&mut self, entry: FileEntry) -> Result<()> {
        if self.contains_path(&entry.path) {
            bail!("manifest already contains {}", entry.path);
        }
        self.files.push(entry);
        self.files.sort_by(|left, right| left.path.cmp(&right.path));
        self.validate()
    }

    pub fn persist(&self, root: &Path) -> Result<()> {
        fs::create_dir_all(root).with_context(|| format!("create {}", root.display()))?;
        let final_path = root.join("manifest.json");
        let temporary_path = root.join("manifest.json.tmp");
        let encoded = serde_json::to_vec_pretty(self).context("encode manifest")?;
        let mut file = File::create(&temporary_path)
            .with_context(|| format!("create {}", temporary_path.display()))?;
        file.write_all(&encoded)
            .with_context(|| format!("write {}", temporary_path.display()))?;
        file.write_all(b"\n")
            .with_context(|| format!("finish {}", temporary_path.display()))?;
        file.sync_all()
            .with_context(|| format!("sync {}", temporary_path.display()))?;
        fs::rename(&temporary_path, &final_path).with_context(|| {
            format!(
                "atomically rename {} to {}",
                temporary_path.display(),
                final_path.display()
            )
        })?;
        sync_directory(root)?;
        Ok(())
    }
}

pub fn sync_directory(path: &Path) -> Result<()> {
    let directory =
        File::open(path).with_context(|| format!("open directory {}", path.display()))?;
    directory
        .sync_all()
        .with_context(|| format!("sync directory {}", path.display()))
}

pub fn relative_path(root: &Path, path: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(root)
        .with_context(|| format!("{} is outside {}", path.display(), root.display()))?;
    path_to_manifest_string(relative)
}

fn path_to_manifest_string(path: &Path) -> Result<String> {
    let value = path
        .to_str()
        .context("output path is not valid UTF-8")?
        .replace(std::path::MAIN_SEPARATOR, "/");
    if value.starts_with('/') || value.contains("..") {
        bail!("unsafe manifest path {value:?}");
    }
    Ok(value)
}

pub fn resolve_manifest_path(root: &Path, value: &str) -> Result<PathBuf> {
    if value.starts_with('/') || value.split('/').any(|component| component == "..") {
        bail!("unsafe manifest path {value:?}");
    }
    Ok(root.join(value))
}
