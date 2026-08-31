use rusteron_archive::bindings::AERON_NULL_POSITION;
use rusteron_archive::{
    AeronArchiveRecordingDescriptor, AeronArchiveRecordingDescriptorConsumerFuncCallback,
    AeronUriStringBuilder, IntoCString,
};
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

#[derive(Debug)]
pub struct RecorderDescriptorReader {
    pub last_recording: Option<RecordingInfo>,
    pub skipped_empty: usize,
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
        Self {
            last_recording: None,
            skipped_empty: 0,
            skipped_invalid: 0,
        }
    }
}

pub fn is_live_recording(stop_position: i64) -> bool {
    stop_position == i64::from(AERON_NULL_POSITION)
}

fn is_replayable_recording(start_position: i64, stop_position: i64) -> bool {
    is_live_recording(stop_position) || (stop_position > 0 && start_position < stop_position)
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
        if is_replayable_recording(
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
            let recording_info = RecordingInfo {
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
            };
            self.last_recording = Some(recording_info);
        } else {
            if recording_descriptor.start_position == recording_descriptor.stop_position {
                self.skipped_empty += 1;
            } else {
                self.skipped_invalid += 1;
            }
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

    #[test]
    fn null_stop_position_is_live() {
        assert!(is_live_recording(i64::from(AERON_NULL_POSITION)));
        assert!(!is_live_recording(0));
        assert!(!is_live_recording(1));
    }

    #[test]
    fn live_and_non_empty_stopped_recordings_are_replayable() {
        assert!(is_replayable_recording(0, i64::from(AERON_NULL_POSITION)));
        assert!(is_replayable_recording(0, 96));
        assert!(!is_replayable_recording(0, 0));
        assert!(!is_replayable_recording(96, 96));
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
        let reader = RecorderDescriptorReader {
            last_recording: None,
            skipped_empty: 2,
            skipped_invalid: 0,
        };

        assert!(reader.last_recording.is_none());
        assert_eq!(reader.skipped_empty, 2);
        assert_eq!(reader.skipped_invalid, 0);
        assert!(ensure_replayable_recording(reader.skipped_invalid).is_ok());
    }

    #[test]
    fn corrupt_recording_is_fatal() {
        assert!(!is_replayable_recording(96, 95));
        assert!(ensure_replayable_recording(1).is_err());
    }

    #[test]
    fn live_descriptor_requires_release_before_replay() {
        assert!(is_live_recording(i64::from(AERON_NULL_POSITION)));
    }
}
