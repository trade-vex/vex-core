use rusteron_archive::{
    AeronArchiveRecordingDescriptor, AeronArchiveRecordingDescriptorConsumerFuncCallback,
};
use tracing::debug;

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
                "Found a Recording: {}",
                recording_descriptor.recording_id
            );
            // performing a deep copy here,
            // this is very important, without this, the last recording will point to a dangling reference, as memory is deallocated after the callback by aeronc.
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
            debug!("skipping recording as the positions are invalid, start: {}, stop: {}", recording_descriptor.start_position, recording_descriptor.stop_position);
        }
    }
}
