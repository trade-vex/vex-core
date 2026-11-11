//! Engine state snapshots at a journal position. A start loads the newest one at or below the
//! delivered watermark and replays the journal from its position; the journal below an older one
//! may then be purged. The state itself is opaque here (the server crate's `EngineState`).
//!
//! File: `<dir>/snapshot-<position:020>.bin` = `VEXSNAP\x01` | recording id (i64 LE) | position
//! (i64 LE) | last command timestamp (u64 LE) | recording start timestamp (i64 LE) | state |
//! crc32 (u32 LE) of everything before it.

use super::replay::RecordingInfo;
use rusteron_archive::AeronArchive;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use tracing::{error, info, warn};

const MAGIC: &[u8; 8] = b"VEXSNAP\x01";
const HEADER: usize = MAGIC.len() + 8 + 8 + 8 + 8;
/// Snapshots kept: the newest is unusable until the watermark passes it, so a start falls back to
/// the one before.
const KEEP: usize = 2;

/// Serializes the engine's state; the server calls it only while the pipeline is drained.
pub type StateCapture = Box<dyn FnMut() -> Vec<u8> + Send>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotHeader {
    /// The journal's recording.
    pub recording_id: i64,
    /// The journal position every command the state holds ends at or before; a replay resumes
    /// here.
    pub position: i64,
    /// Timestamp of the last command the state holds.
    pub last_timestamp: u64,
    /// When the archive started the recording (epoch ms): a wiped archive numbers its recordings
    /// from 0 again, so the id alone does not name the journal.
    pub recording_started: i64,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub header: SnapshotHeader,
    pub state: Vec<u8>,
}

/// Writes `snapshot` crash-safely (temp file, fsync, rename, fsync of the directory), then keeps it
/// and the newest snapshot below it ([`KEEP`] in all) and deletes the others and any temp file. One
/// above it is another journal's: this journal's positions only grow. Returns the bytes written.
pub fn write(dir: &Path, snapshot: &Snapshot) -> io::Result<usize> {
    fs::create_dir_all(dir)?;
    let path = dir.join(format!("snapshot-{:020}.bin", snapshot.header.position));
    let tmp = path.with_extension("bin.tmp");
    let mut bytes = Vec::with_capacity(HEADER + snapshot.state.len() + 4);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&snapshot.header.recording_id.to_le_bytes());
    bytes.extend_from_slice(&snapshot.header.position.to_le_bytes());
    bytes.extend_from_slice(&snapshot.header.last_timestamp.to_le_bytes());
    bytes.extend_from_slice(&snapshot.header.recording_started.to_le_bytes());
    bytes.extend_from_slice(&snapshot.state);
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_le_bytes());
    let mut file = File::create(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&tmp, &path)?;
    File::open(dir)?.sync_all()?;
    let mut kept = 0;
    for (position, path) in files(dir)? {
        if position <= snapshot.header.position && kept < KEEP {
            kept += 1;
        } else {
            fs::remove_file(path)?;
        }
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "tmp") {
            fs::remove_file(path)?;
        }
    }
    Ok(bytes.len())
}

/// The snapshot files in `dir`, newest (highest position) first. Other names are not snapshots.
fn files(dir: &Path) -> io::Result<Vec<(i64, PathBuf)>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let position = path
            .file_name()
            .and_then(|name| {
                name.to_str()?
                    .strip_prefix("snapshot-")?
                    .strip_suffix(".bin")
            })
            .and_then(|position| position.parse::<i64>().ok());
        if let Some(position) = position {
            files.push((position, path));
        }
    }
    files.sort_unstable_by_key(|(position, _)| std::cmp::Reverse(*position));
    Ok(files)
}

fn parse(bytes: &[u8]) -> Option<Snapshot> {
    let (body, crc) = bytes.split_at_checked(bytes.len().checked_sub(4)?)?;
    if body.len() < HEADER || !body.starts_with(MAGIC) || crc32fast::hash(body).to_le_bytes() != crc
    {
        return None;
    }
    let field = |offset: usize| -> [u8; 8] { body[offset..offset + 8].try_into().unwrap() };
    Some(Snapshot {
        header: SnapshotHeader {
            recording_id: i64::from_le_bytes(field(8)),
            position: i64::from_le_bytes(field(16)),
            last_timestamp: u64::from_le_bytes(field(24)),
            recording_started: i64::from_le_bytes(field(32)),
        },
        state: body[HEADER..].to_vec(),
    })
}

/// Every readable snapshot in `dir`, newest first; a missing directory holds none. A file that
/// fails the magic, length or checksum is logged and skipped, never deleted here: the journal
/// holds everything it held, and [`write`] deletes it once [`KEEP`] newer ones exist.
pub fn read_all(dir: &Path) -> io::Result<Vec<Snapshot>> {
    let files = match files(dir) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        files => files?,
    };
    let mut snapshots = Vec::new();
    for (_, path) in files {
        match parse(&fs::read(&path)?) {
            Some(snapshot) => snapshots.push(snapshot),
            None => error!(
                target: "snapshot",
                action = "snapshot_corrupt",
                path = %path.display(),
                "unreadable snapshot skipped; the start falls back to an older one or the journal"
            ),
        }
    }
    Ok(snapshots)
}

/// The snapshot a start may load: the newest whose last command is at or below the delivered
/// watermark. A newer one holds commands whose events Kafka may lack, which only a replay
/// re-publishes.
pub fn choose(snapshots: Vec<Snapshot>, delivered: u64) -> Option<Snapshot> {
    snapshots.into_iter().find(|snapshot| {
        let usable = snapshot.header.last_timestamp <= delivered;
        if !usable {
            warn!(
                target: "snapshot",
                action = "snapshot_ahead_of_watermark",
                position = snapshot.header.position,
                last_timestamp = snapshot.header.last_timestamp,
                delivered
            );
        }
        usable
    })
}

/// [`read_all`] then [`choose`].
pub fn load(dir: &Path, delivered: u64) -> io::Result<Option<Snapshot>> {
    let loaded = choose(read_all(dir)?, delivered);
    if let Some(snapshot) = &loaded {
        info!(
            target: "snapshot",
            action = "snapshot_loaded",
            recording_id = snapshot.header.recording_id,
            position = snapshot.header.position,
            last_timestamp = snapshot.header.last_timestamp,
            delivered
        );
    }
    Ok(loaded)
}

/// Where the journal may be purged once a newer snapshot than `previous` is taken: the base of
/// the segment that holds `previous`'s position, once `previous` is at or below the delivered
/// watermark, so every later start can load it (or the newer one). None while that base is the
/// recording's start, as Aeron purges whole segments only.
pub(super) fn purge_target(
    recording: &RecordingInfo,
    previous: SnapshotHeader,
    delivered: u64,
) -> Option<i64> {
    (previous.last_timestamp <= delivered)
        .then(|| {
            AeronArchive::segment_file_base_position(
                recording.start_position,
                previous.position,
                recording.term_buffer_length,
                recording.segment_file_length,
            )
        })
        .filter(|&base| base > recording.start_position)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("vex-snapshot-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn snapshot(position: i64, last_timestamp: u64) -> Snapshot {
        Snapshot {
            header: SnapshotHeader {
                recording_id: 7,
                position,
                last_timestamp,
                recording_started: 1_000,
            },
            state: format!("state at {position}").into_bytes(),
        }
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<_> = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn write_keeps_the_two_newest_and_reads_newest_first() {
        let dir = dir("keep");
        for position in [96, 960, 9_600] {
            write(&dir, &snapshot(position, position as u64)).unwrap();
        }
        assert_eq!(
            names(&dir),
            [
                "snapshot-00000000000000000960.bin",
                "snapshot-00000000000000009600.bin"
            ]
        );
        assert_eq!(
            read_all(&dir).unwrap(),
            [snapshot(9_600, 9_600), snapshot(960, 960)]
        );

        // Below both: they are another journal's (a ledger reset that kept them).
        write(&dir, &snapshot(480, 480)).unwrap();
        assert_eq!(read_all(&dir).unwrap(), [snapshot(480, 480)]);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_corrupt_or_foreign_file_is_skipped_and_logged() {
        let dir = dir("corrupt");
        write(&dir, &snapshot(96, 1)).unwrap();
        write(&dir, &snapshot(192, 2)).unwrap();
        let newest = dir.join("snapshot-00000000000000000192.bin");
        let mut bytes = fs::read(&newest).unwrap();
        bytes[HEADER] ^= 1;
        fs::write(&newest, bytes).unwrap();
        fs::write(dir.join("snapshot-x.bin"), b"garbage").unwrap();
        fs::write(dir.join("snapshot-00000000000000000288.bin"), b"short").unwrap();
        assert_eq!(read_all(&dir).unwrap(), [snapshot(96, 1)]);

        // A write removes a stale temp file, and corrupt snapshots below the two newest; a name
        // that is not a snapshot's is left alone.
        fs::write(
            dir.join("snapshot-00000000000000000384.bin.tmp"),
            b"partial",
        )
        .unwrap();
        write(&dir, &snapshot(480, 5)).unwrap();
        assert_eq!(
            names(&dir),
            [
                "snapshot-00000000000000000288.bin",
                "snapshot-00000000000000000480.bin",
                "snapshot-x.bin"
            ]
        );
        assert_eq!(read_all(&dir).unwrap(), [snapshot(480, 5)]);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn choose_takes_the_newest_at_or_below_the_watermark() {
        let snapshots = || vec![snapshot(960, 30), snapshot(96, 20)];
        let chosen = |delivered| choose(snapshots(), delivered).map(|s| s.header.position);
        assert_eq!(chosen(25), Some(96));
        assert_eq!(chosen(19), None);
        assert_eq!(chosen(30), Some(960));
    }

    #[test]
    fn a_missing_directory_reads_as_no_snapshot() {
        let dir = dir("missing");
        assert_eq!(read_all(&dir).unwrap(), []);
        assert_eq!(load(&dir, u64::MAX).unwrap(), None);
    }

    #[test]
    fn purge_target_needs_the_previous_snapshot_delivered_and_a_full_segment() {
        const SEGMENT: i64 = 4 * 65_536;
        let recording = RecordingInfo {
            recording_id: 7,
            start_position: 0,
            stop_position: -1,
            initial_term_id: 1,
            term_buffer_length: 65_536,
            segment_file_length: SEGMENT as i32,
            session_id: 17,
            start_timestamp: 1_000,
        };
        let previous = SnapshotHeader {
            recording_id: 7,
            position: 2 * SEGMENT + 960,
            last_timestamp: 30,
            recording_started: 1_000,
        };
        assert_eq!(purge_target(&recording, previous, 30), Some(2 * SEGMENT));
        assert_eq!(purge_target(&recording, previous, 29), None);
        // Still in the first segment: nothing whole to purge.
        let early = SnapshotHeader {
            position: SEGMENT - 96,
            ..previous
        };
        assert_eq!(purge_target(&recording, early, 30), None);
        // Purged already up to that segment.
        let purged = RecordingInfo {
            start_position: 2 * SEGMENT,
            ..recording
        };
        assert_eq!(purge_target(&purged, previous, 30), None);
    }
}
