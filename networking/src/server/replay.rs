use rusteron_archive::bindings::AERON_NULL_POSITION;
use rusteron_archive::{
    AeronArchiveRecordingDescriptor, AeronArchiveRecordingDescriptorConsumerFuncCallback,
    AeronUriStringBuilder, IntoCString,
};
use std::time::{Duration, Instant};
use tracing::debug;

use crate::server::{RECORDING_CHANNEL, ServerError};

pub struct ExtendedRecordingDescriptor {
    pub recording_id: i64,
    pub channel: String,
}

impl ExtendedRecordingDescriptor {
    pub fn new(
        initial_term_id: i32,
        position: i64,
        term_length: i32,
        recording_id: i64,
    ) -> Result<Self, ServerError> {
        let uri_builder = AeronUriStringBuilder::default();
        uri_builder.init_on_string(&RECORDING_CHANNEL.into_c_string())?;
        uri_builder.set_initial_position(position, initial_term_id, term_length)?;
        let channel = uri_builder.build(128)?;
        uri_builder.close()?;
        Ok(Self {
            recording_id,
            channel,
        })
    }
}

#[derive(Debug)]
#[allow(unused)]
pub struct RecordingInfo {
    pub control_session_id: i64,
    pub correlation_id: i64,
    pub recording_id: i64,
    pub start_timestamp: i64,
    pub stop_timestamp: i64,
    pub start_position: i64,
    pub stop_position: i64,
    pub initial_term_id: i32,
    pub segment_file_length: i32,
    pub term_buffer_length: i32,
    pub mtu_length: i32,
    pub session_id: i32,
    pub stream_id: i32,
    pub stripped_channel_length: usize,
    pub original_channel_length: usize,
    pub source_identity_length: usize,
}

#[derive(Debug, Default)]
pub struct RecorderDescriptorReader {
    /// Recordings with valid positions, oldest first: live, empty or holding commands.
    pub recordings: Vec<RecordingInfo>,
    pub skipped_invalid: usize,
}

#[derive(Debug)]
pub struct RecordingCounter;

impl AeronArchiveRecordingDescriptorConsumerFuncCallback for RecordingCounter {
    fn handle_aeron_archive_recording_descriptor_consumer_func(
        &mut self,
        _recording_descriptor: AeronArchiveRecordingDescriptor,
    ) {
    }
}

impl RecorderDescriptorReader {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Returns the recording's final position. A live descriptor belongs to a process killed while
/// recording; the stop issued before listing ends that recording, so wait for its final position.
pub(super) fn release_recording(
    record: &RecordingInfo,
    await_stop: impl FnOnce(i64) -> Result<i64, ServerError>,
) -> Result<i64, ServerError> {
    let stop = if is_live_recording(record.stop_position) {
        await_stop(record.recording_id)?
    } else {
        record.stop_position
    };
    if stop < record.start_position {
        return Err(ServerError::ReplayError(format!(
            "recording {} has invalid positions: start {}, stop {stop}",
            record.recording_id, record.start_position
        )));
    }
    Ok(stop)
}

/// The image cursor includes padding, including polls which deliver no data fragments.
pub(super) fn drain_replay(
    start: i64,
    stop: i64,
    timeout: Duration,
    mut poll: impl FnMut() -> Result<(i64, bool), ServerError>,
) -> Result<(), ServerError> {
    let mut position = start;
    let mut last_progress = Instant::now();
    while position < stop {
        let (next, ended) = poll()?;
        if next < position {
            return Err(ServerError::ReplayError(
                "replay image position regressed".into(),
            ));
        }
        if next > position {
            last_progress = Instant::now();
            position = next;
        }
        if ended {
            break;
        }
        if position < stop && last_progress.elapsed() >= timeout {
            return Err(ServerError::ReplayError("replay image stalled".into()));
        }
        std::hint::spin_loop();
    }
    assert_replay_position_complete(position, stop)
}

pub fn is_live_recording(stop_position: i64) -> bool {
    stop_position == i64::from(AERON_NULL_POSITION)
}

fn is_valid_recording(start_position: i64, stop_position: i64) -> bool {
    is_live_recording(stop_position) || start_position <= stop_position
}

/// Whether a released recording (see [`release_recording`]) holds any command.
fn holds_commands(record: &RecordingInfo) -> bool {
    record.stop_position > record.start_position
}

/// The recording a start replays and extends: the newest, unless it holds no command. A newer
/// empty recording means a run started fresh and journaled nothing; replaying an older one would
/// resurrect the lineage that run discarded. Takes released recordings, oldest first.
pub fn replay_lineage(recordings: &[RecordingInfo]) -> Option<&RecordingInfo> {
    recordings.last().filter(|record| holds_commands(record))
}

/// The recording that holds the last command in the journal: the newest one holding any. A newer
/// empty recording discards an older lineage's state, not the ids and timestamps it issued.
/// Takes released recordings, oldest first.
pub fn tail_recording(recordings: &[RecordingInfo]) -> Option<&RecordingInfo> {
    recordings
        .iter()
        .rev()
        .find(|record| holds_commands(record))
}

/// Where reading a released recording's last command starts: the start of the term that holds
/// the recording's last byte, or the recording's own start if that is later. Both are frame
/// boundaries. A term ends in padding only where the next command did not fit, so the term that
/// holds the last byte also holds the last command, and finding it replays at most one term.
pub(super) fn tail_window_start(record: &RecordingInfo) -> i64 {
    let term_length = i64::from(record.term_buffer_length);
    let last_byte = record.stop_position - 1;
    (last_byte - last_byte % term_length).max(record.start_position)
}

pub fn ensure_replayable_recording(skipped_invalid: usize) -> Result<(), ServerError> {
    if skipped_invalid > 0 {
        return Err(ServerError::ReplayError(format!(
            "archive contains {skipped_invalid} recording(s) with invalid positions"
        )));
    }
    Ok(())
}

pub fn assert_replay_complete(
    recorded_command_count: i64,
    consumed_command_count: i64,
) -> Result<(), ServerError> {
    if recorded_command_count != consumed_command_count {
        return Err(ServerError::ReplayError(format!(
            "replay incomplete: recorded {recorded_command_count} command(s), consumed {consumed_command_count}"
        )));
    }
    Ok(())
}

pub fn assert_replay_position_complete(
    consumed_position: i64,
    stop_position: i64,
) -> Result<(), ServerError> {
    if consumed_position < stop_position {
        return Err(ServerError::ReplayError(format!(
            "replay incomplete: consumed position {consumed_position}, stop position {stop_position}"
        )));
    }
    Ok(())
}

pub fn fail_on_replay_error(replay_error: Option<&str>) -> Result<(), ServerError> {
    if let Some(replay_error) = replay_error {
        return Err(ServerError::ReplayError(replay_error.to_string()));
    }
    Ok(())
}

impl AeronArchiveRecordingDescriptorConsumerFuncCallback for RecorderDescriptorReader {
    fn handle_aeron_archive_recording_descriptor_consumer_func(
        &mut self,
        recording_descriptor: AeronArchiveRecordingDescriptor,
    ) {
        if is_valid_recording(
            recording_descriptor.start_position,
            recording_descriptor.stop_position,
        ) {
            debug!(
                target: "replay",
                action = "recording_found",
                recording_id = recording_descriptor.recording_id,
                start_position = recording_descriptor.start_position,
                stop_position = recording_descriptor.stop_position
            );
            // Performing a deep copy here is essential;
            // the descriptor lifetime ends after the callback.
            self.recordings.push(RecordingInfo {
                control_session_id: recording_descriptor.control_session_id,
                correlation_id: recording_descriptor.correlation_id,
                recording_id: recording_descriptor.recording_id,
                start_timestamp: recording_descriptor.start_timestamp,
                stop_timestamp: recording_descriptor.stop_timestamp,
                start_position: recording_descriptor.start_position,
                stop_position: recording_descriptor.stop_position,
                initial_term_id: recording_descriptor.initial_term_id,
                segment_file_length: recording_descriptor.segment_file_length,
                term_buffer_length: recording_descriptor.term_buffer_length,
                mtu_length: recording_descriptor.mtu_length,
                session_id: recording_descriptor.session_id,
                stream_id: recording_descriptor.stream_id,
                stripped_channel_length: recording_descriptor.stripped_channel_length,
                original_channel_length: recording_descriptor.original_channel_length,
                source_identity_length: recording_descriptor.source_identity_length,
            });
        } else {
            self.skipped_invalid += 1;
            debug!(
                target: "replay",
                action = "recording_skipped",
                start_position = recording_descriptor.start_position,
                stop_position = recording_descriptor.stop_position,
                "recording has invalid positions"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::FRAMESIZE;

    const TERM_LENGTH: i64 = 65_536;

    fn recording(start_position: i64, stop_position: i64) -> RecordingInfo {
        RecordingInfo {
            control_session_id: 0,
            correlation_id: 0,
            recording_id: 7,
            start_timestamp: 0,
            stop_timestamp: 0,
            start_position,
            stop_position,
            initial_term_id: 1,
            segment_file_length: TERM_LENGTH as i32,
            term_buffer_length: TERM_LENGTH as i32,
            mtu_length: 1408,
            session_id: 17,
            stream_id: 2001,
            stripped_channel_length: 0,
            original_channel_length: 0,
            source_identity_length: 0,
        }
    }

    #[test]
    fn c8_1_replay_across_term_boundary_includes_padding() {
        let term = 65_536;
        let frames_in_term = term / FRAMESIZE;
        let stop = term + FRAMESIZE;
        let mut frames = 0;
        drain_replay(0, stop, Duration::from_secs(1), || {
            frames += 1;
            let position = if frames <= frames_in_term {
                frames * FRAMESIZE
            } else {
                stop
            };
            Ok((position, frames == frames_in_term + 1))
        })
        .unwrap();
        assert_eq!(frames, frames_in_term + 1);
        assert_ne!(frames * FRAMESIZE, stop);
    }

    #[test]
    fn c8_1_truncated_and_stalled_replays_fail_without_hanging() {
        let mut polls = 0;
        let error = drain_replay(0, FRAMESIZE * 2, Duration::from_secs(1), || {
            polls += 1;
            assert_eq!(polls, 1, "ended replay must not poll again");
            Ok((FRAMESIZE, true))
        })
        .unwrap_err();
        assert!(error.to_string().contains("replay incomplete"));
        assert!(drain_replay(0, FRAMESIZE, Duration::ZERO, || Ok((0, false))).is_err());
        // A padding-only poll still advances the image cursor.
        drain_replay(65_472, 65_536, Duration::ZERO, || Ok((65_536, true))).unwrap();
    }

    #[test]
    fn c8_3_live_recording_is_awaited_and_stopped_recording_is_used_as_is() {
        let record = |stop_position| recording(0, stop_position);
        let live = record(i64::from(AERON_NULL_POSITION));

        let stop = release_recording(&live, |id| {
            assert_eq!(id, 7);
            Ok(FRAMESIZE)
        })
        .unwrap();
        assert_eq!(stop, FRAMESIZE);
        assert!(
            release_recording(&live, |_| Err(ServerError::ReplayError(
                "not released".into()
            )))
            .is_err()
        );
        assert!(release_recording(&live, |_| Ok(-1)).is_err());

        let stop = release_recording(&record(FRAMESIZE * 2), |_| {
            panic!("a stopped recording has its final position already")
        })
        .unwrap();
        assert_eq!(stop, FRAMESIZE * 2);
    }

    #[test]
    fn null_stop_position_is_live() {
        assert!(is_live_recording(i64::from(AERON_NULL_POSITION)));
        assert!(!is_live_recording(0));
        assert!(!is_live_recording(1));
    }

    #[test]
    fn live_empty_and_non_empty_recordings_are_valid() {
        assert!(is_valid_recording(0, i64::from(AERON_NULL_POSITION)));
        assert!(is_valid_recording(0, 96));
        assert!(is_valid_recording(0, 0));
        assert!(is_valid_recording(96, 96));
    }

    #[test]
    fn replay_completion_requires_every_recorded_command() {
        assert!(assert_replay_complete(2, 2).is_ok());
        assert!(assert_replay_complete(2, 1).is_err());
        assert!(assert_replay_complete(1, 2).is_err());
    }

    #[test]
    fn replay_position_must_reach_stop_position_even_with_padding() {
        let stop_position = FRAMESIZE * 2;
        assert!(assert_replay_position_complete(stop_position, stop_position).is_ok());
        assert!(assert_replay_position_complete(stop_position - 1, stop_position).is_err());

        let padded_stop_position = stop_position + 1;
        assert_ne!(padded_stop_position % FRAMESIZE, 0);
        assert!(
            assert_replay_position_complete(padded_stop_position, padded_stop_position).is_ok()
        );
    }

    #[test]
    fn recorded_decode_error_aborts_replay() {
        let message = "failed to decode replay order command";
        let result = fail_on_replay_error(Some(message));

        assert!(matches!(
            result,
            Err(ServerError::ReplayError(replay_error)) if replay_error == message
        ));
    }

    #[test]
    fn empty_archive_is_allowed() {
        assert!(ensure_replayable_recording(0).is_ok());
    }

    #[test]
    fn archive_with_only_empty_recordings_is_allowed() {
        let reader = listed(&[(1, 0, 0), (2, FRAMESIZE, FRAMESIZE)]);

        assert_eq!(reader.recordings.len(), 2);
        assert_eq!(reader.skipped_invalid, 0);
        assert!(ensure_replayable_recording(reader.skipped_invalid).is_ok());
        assert!(replay_lineage(&reader.recordings).is_none());
        assert!(tail_recording(&reader.recordings).is_none());
    }

    fn descriptor(recording_id: i64, start: i64, stop: i64) -> AeronArchiveRecordingDescriptor {
        AeronArchiveRecordingDescriptor::new(
            0,
            0,
            recording_id,
            0,
            0,
            start,
            stop,
            1,
            65_536,
            65_536,
            1408,
            17,
            2001,
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            0,
        )
        .unwrap()
    }

    /// The archive's `(recording_id, start, stop)` descriptors, oldest first, as the reader keeps
    /// them.
    fn listed(descriptors: &[(i64, i64, i64)]) -> RecorderDescriptorReader {
        let mut reader = RecorderDescriptorReader::new();
        for &(recording_id, start, stop) in descriptors {
            reader.handle_aeron_archive_recording_descriptor_consumer_func(descriptor(
                recording_id,
                start,
                stop,
            ));
        }
        reader
    }

    fn replayed(descriptors: &[(i64, i64, i64)]) -> Option<i64> {
        replay_lineage(&listed(descriptors).recordings).map(|record| record.recording_id)
    }

    fn tail(descriptors: &[(i64, i64, i64)]) -> Option<i64> {
        tail_recording(&listed(descriptors).recordings).map(|record| record.recording_id)
    }

    #[test]
    fn newer_empty_recording_is_not_bypassed_for_an_older_journal() {
        let live = i64::from(AERON_NULL_POSITION);
        assert_eq!(replayed(&[(1, 0, FRAMESIZE), (2, 0, 0)]), None);
        assert_eq!(replayed(&[(1, 0, live), (2, 0, 0)]), None);
        assert_eq!(replayed(&[(1, 0, 0), (2, 0, FRAMESIZE)]), Some(2));
        assert_eq!(
            replayed(&[(1, 0, FRAMESIZE), (2, 0, 0), (3, 0, FRAMESIZE)]),
            Some(3)
        );
    }

    #[test]
    fn journal_tail_is_in_the_newest_recording_holding_commands() {
        assert_eq!(tail(&[]), None);
        assert_eq!(tail(&[(1, 0, 0)]), None);
        // A fresh start that journaled nothing does not hide the ids issued before it.
        assert_eq!(tail(&[(1, 0, FRAMESIZE), (2, 0, 0)]), Some(1));
        assert_eq!(
            tail(&[(1, 0, FRAMESIZE), (2, 0, 0), (3, 0, FRAMESIZE), (4, 0, 0)]),
            Some(3)
        );
    }

    #[test]
    fn journal_tail_is_read_from_the_term_that_holds_it() {
        let window = |start, stop| tail_window_start(&recording(start, stop));

        assert_eq!(window(0, 3 * FRAMESIZE), 0);
        // The first term ended in padding and the last command opened the second.
        assert_eq!(window(0, TERM_LENGTH + FRAMESIZE), TERM_LENGTH);
        // Ends in padding: the command that did not fit was never recorded.
        assert_eq!(window(0, TERM_LENGTH), 0);
        assert_eq!(window(0, 2 * TERM_LENGTH + 5 * FRAMESIZE), 2 * TERM_LENGTH);
        // Never before the recording's own start.
        assert_eq!(window(10 * FRAMESIZE, 12 * FRAMESIZE), 10 * FRAMESIZE);
    }

    #[test]
    fn corrupt_recording_is_fatal() {
        assert!(!is_valid_recording(96, 95));
        let reader = listed(&[(1, 96, 95)]);
        assert!(reader.recordings.is_empty());
        assert!(ensure_replayable_recording(reader.skipped_invalid).is_err());
    }

    #[test]
    fn live_descriptor_requires_release_before_replay() {
        assert!(is_live_recording(i64::from(AERON_NULL_POSITION)));
    }
}
