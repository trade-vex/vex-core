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
}

impl RecorderDescriptorReader {
    pub fn new() -> Self {
        Self {
            last_recording: None,
        }
    }
}

impl AeronArchiveRecordingDescriptorConsumerFuncCallback for RecorderDescriptorReader {
    fn handle_aeron_archive_recording_descriptor_consumer_func(
        &mut self,
        recording_descriptor: AeronArchiveRecordingDescriptor,
    ) {
        if recording_descriptor.stop_position > 0
            && recording_descriptor.start_position < recording_descriptor.stop_position
        {
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

/// Reader that captures active recordings (where stop_position == 0)
#[derive(Debug)]
pub struct ActiveRecordingReader {
    pub active_recording: Option<RecordingInfo>,
}

impl ActiveRecordingReader {
    pub fn new() -> Self {
        Self {
            active_recording: None,
        }
    }
}

impl AeronArchiveRecordingDescriptorConsumerFuncCallback for ActiveRecordingReader {
    fn handle_aeron_archive_recording_descriptor_consumer_func(
        &mut self,
        recording_descriptor: AeronArchiveRecordingDescriptor,
    ) {
        // Active recordings have stop_position == 0
        if recording_descriptor.stop_position == 0 {
            debug!(
                target: "recording",
                action = "active_recording_found",
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
            self.active_recording = Some(recording_info);
        }
    }
}
