//! Live regressions: use a dedicated, disposable archive/driver; run with --test-threads=1.
//! VEX_RECOVERY_AERON_DIR and VEX_RECOVERY_ARCHIVE_CHANNEL select that instance.
//! C7-1 additionally needs VEX_RECOVERY_ARCHIVE_PID (a separate Unix archive process).

use super::*;
use common::{FRAMESIZE, ORDERCOMMANDSIZE, encode_order_command};
use disruptor::{BusySpin, Producer, build_multi_producer};
use rusteron_archive::{AeronPublication, AeronReservedValueSupplierLogger, RecordingPos};
use std::sync::{Mutex, mpsc};

struct Fixture {
    config: CoreNetworkingConfig,
    aeron: Aeron,
    archive: AeronArchive,
}

impl Fixture {
    fn new() -> Self {
        let mut config = CoreNetworkingConfig::test_defaults();
        config.context_dir = std::env::var("VEX_RECOVERY_AERON_DIR")
            .expect("set VEX_RECOVERY_AERON_DIR to a dedicated media driver directory");
        config.request_control_channel = std::env::var("VEX_RECOVERY_ARCHIVE_CHANNEL")
            .expect("set VEX_RECOVERY_ARCHIVE_CHANNEL to a disposable archive control channel");
        config.enable_authentication = false;
        let aeron = VexCoreServer::initialize_aeron(&config).unwrap();
        let archive = VexCoreServer::initialize_archive(&config, &aeron).unwrap();
        Self {
            config,
            aeron,
            archive,
        }
    }

    fn recording(&self, commands: usize) -> (i64, i64, AeronPublication) {
        let channel = "aeron:ipc?term-length=65536".into_c_string();
        // Deliberately reproduce the legacy subscription which survives publication loss.
        let subscription = self
            .archive
            .start_recording(
                &channel,
                RECORDING_STREAM_ID,
                SourceLocation::AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
                false,
            )
            .unwrap();
        let publication = self
            .aeron
            .add_publication(&channel, RECORDING_STREAM_ID, DRAIN_TIMEOUT)
            .unwrap();
        wait_for_startup_connection("test publication", || publication.is_connected()).unwrap();
        let counters = self.aeron.counters_reader();
        let mut recording_id = None;
        wait_until(DRAIN_TIMEOUT, || {
            let counter =
                RecordingPos::find_counter_id_by_session(&counters, publication.session_id());
            if counter != AERON_NULL_COUNTER_ID {
                recording_id = Some(RecordingPos::get_recording_id(&counters, counter)?);
            }
            Ok(recording_id.is_some())
        })
        .unwrap();
        for id in 0..commands {
            let command = OrderCommand {
                order_id: id as u64 + 1,
                ..OrderCommand::default()
            };
            let mut bytes = [0; ORDERCOMMANDSIZE];
            encode_order_command(&command, &mut bytes).unwrap();
            wait_until(DRAIN_TIMEOUT, || {
                Ok(publication.offer::<AeronReservedValueSupplierLogger>(&bytes, None) >= 0)
            })
            .unwrap();
        }
        let id = recording_id.unwrap();
        wait_until(DRAIN_TIMEOUT, || {
            Ok(self.archive.get_recording_position(id)? >= publication.position())
        })
        .unwrap();
        (id, subscription, publication)
    }

    fn replay(
        &self,
        publications: Arc<Publications>,
        producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
    ) -> ExtendedRecordingDescriptor {
        VexCoreServer::start_replay(
            &self.aeron,
            &self.archive,
            producer,
            Arc::new(AtomicBool::new(false)),
            publications,
        )
        .unwrap()
        .unwrap()
    }
}

fn collector(
    publications: Arc<Publications>,
    received: Arc<Mutex<Vec<u64>>>,
) -> MultiProducer<OrderCommand, SingleConsumerBarrier> {
    build_multi_producer(1024, OrderCommand::default, BusySpin)
        .handle_events_with(move |cell, sequence, _| {
            // SAFETY: Sole consumer reads its currently owned slot.
            received
                .lock()
                .unwrap()
                .push(unsafe { (*cell.get()).order_id });
            publications.completed(sequence);
        })
        .build()
}

#[test]
#[ignore = "needs dedicated live Aeron driver/archive; set VEX_RECOVERY_AERON_DIR and VEX_RECOVERY_ARCHIVE_CHANNEL"]
fn c8_1_live_term_rotation_and_truncated_replay() {
    let fixture = Fixture::new();
    let count = (65_536 / FRAMESIZE + 1) as usize;
    let (id, subscription_id, publication) = fixture.recording(count);
    let stop = publication.position();
    assert_eq!(stop, 65_536 + FRAMESIZE);
    fixture
        .archive
        .stop_recording_subscription(subscription_id)
        .unwrap();
    VexCoreServer::wait_for_recording_stop(&fixture.archive, id).unwrap();
    publication.close::<AeronNotificationLogger>(None).unwrap();
    let publications = Arc::new(Publications::new());
    let received = Arc::new(Mutex::new(Vec::new()));
    let producer = collector(Arc::clone(&publications), Arc::clone(&received));
    assert_eq!(
        fixture.replay(publications, producer.clone()).recording_id,
        id
    );
    assert_eq!(received.lock().unwrap().len(), count);

    // A real replay image ends one command below the target. The same production
    // drain loop must return an incomplete-recovery error, without another poll.
    let params =
        AeronArchiveReplayParams::new(AERON_NULL_COUNTER_ID, i32::MAX, 0, stop - FRAMESIZE, 0, 0)
            .unwrap();
    let session = fixture
        .archive
        .start_replay(
            id,
            &RECORDING_CHANNEL.into_c_string(),
            REPLAY_STREAM_ID,
            &params,
        )
        .unwrap() as i32;
    let subscription = fixture
        .aeron
        .add_subscription(
            &format!("aeron:ipc?session-id={session}").into_c_string(),
            REPLAY_STREAM_ID,
            None::<&Handler<AeronAvailableImageLogger>>,
            None::<&Handler<AeronUnavailableImageLogger>>,
            DRAIN_TIMEOUT,
        )
        .unwrap();
    wait_for_startup_connection("truncated test replay", || subscription.is_connected()).unwrap();
    let image = subscription.image_by_session_id(session);
    assert!(!image.get_inner().is_null());
    let error = drain_replay(0, stop, DRAIN_TIMEOUT, || {
        image.poll_once(|_, _| {}, 10)?;
        Ok((
            image.position(),
            image.is_end_of_stream() || image.is_closed(),
        ))
    })
    .unwrap_err();
    subscription.image_release(&image).unwrap();
    subscription.close::<AeronNotificationLogger>(None).unwrap();
    assert!(error.to_string().contains("replay incomplete"));
}

#[test]
#[ignore = "needs dedicated live Aeron driver/archive; set VEX_RECOVERY_AERON_DIR and VEX_RECOVERY_ARCHIVE_CHANNEL"]
fn c8_2_live_held_terminal_second_recovery_has_no_duplicate_journal_entries() {
    let fixture = Fixture::new();
    let (id, sub, publication) = fixture.recording(1);
    let original_stop = publication.position();
    fixture.archive.stop_recording_subscription(sub).unwrap();
    VexCoreServer::wait_for_recording_stop(&fixture.archive, id).unwrap();
    publication.close::<AeronNotificationLogger>(None).unwrap();

    let publications = Arc::new(Publications::new());
    let replay_enabled = Arc::new(AtomicBool::new(true));
    let mode = Arc::clone(&replay_enabled);
    let journal_publications = Arc::clone(&publications);
    let ack = Arc::clone(&publications);
    let (release_journal, journal_gate) = mpsc::channel();
    let (release_terminal, terminal_gate) = mpsc::channel();
    let (entered, terminal_entered) = mpsc::channel();
    let producer = build_multi_producer(64, OrderCommand::default, BusySpin)
        .handle_events_with(move |cell, _, _| {
            journal_gate.recv_timeout(DRAIN_TIMEOUT).unwrap();
            if !mode.load(Ordering::Acquire) {
                // SAFETY: Sole journal consumer; this reproduces the real replay gate.
                journal_publications.publish_to_archive(unsafe { &*cell.get() });
            }
        })
        .and_then()
        .handle_events_with(move |_, sequence, _| {
            entered.send(()).unwrap();
            terminal_gate.recv_timeout(DRAIN_TIMEOUT).unwrap();
            ack.completed(sequence);
        })
        .build();
    let worker_publications = Arc::clone(&publications);
    let worker_mode = Arc::clone(&replay_enabled);
    let (done, completion) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let fixture = Fixture::new();
        let mut server = VexCoreServer::new(
            fixture.config.clone(),
            GatewayAuthenticationKey::default(),
            producer,
            worker_publications,
            true,
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        worker_mode.store(false, Ordering::Release);
        done.send(()).unwrap();
        server.shutdown().unwrap();
    });
    let prematurely_done = completion.recv_timeout(Duration::from_millis(100)).is_ok();
    release_journal.send(()).unwrap();
    terminal_entered.recv_timeout(DRAIN_TIMEOUT).unwrap();
    let held_mode = replay_enabled.load(Ordering::Acquire);
    release_terminal.send(()).unwrap();
    worker.join().unwrap();
    assert!(!prematurely_done);
    assert!(held_mode);
    assert_eq!(
        fixture.archive.get_max_recorded_position(id).unwrap(),
        original_stop
    );
    let second_publications = Arc::new(Publications::new());
    let received = Arc::new(Mutex::new(Vec::new()));
    let second = collector(Arc::clone(&second_publications), Arc::clone(&received));
    assert_eq!(
        fixture
            .replay(second_publications, second.clone())
            .recording_id,
        id
    );
    assert_eq!(*received.lock().unwrap(), [1]);
}

#[test]
#[ignore = "needs dedicated live Aeron driver/archive surviving a child-process crash; set VEX_RECOVERY_AERON_DIR and VEX_RECOVERY_ARCHIVE_CHANNEL"]
fn c8_3_live_crash_restart_extends_legacy_recording() {
    const CHILD: &str = "VEX_RECOVERY_LEGACY_CRASH_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let fixture = Fixture::new();
        let (_id, _sub, _publication) = fixture.recording(1);
        // Abrupt exit: no Rust destructors, no stopRecording, archive stays alive.
        std::process::exit(0);
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "server::recovery_tests::c8_3_live_crash_restart_extends_legacy_recording",
            "--ignored",
            "--exact",
        ])
        .env(CHILD, "1")
        .status()
        .unwrap();
    assert!(status.success());
    let fixture = Fixture::new();
    let mut reader = Handler::leak(RecorderDescriptorReader::new());
    fixture
        .archive
        .list_recordings_for_uri(
            0,
            i32::MAX,
            &RECORDING_CHANNEL.into_c_string(),
            RECORDING_STREAM_ID,
            Some(&reader),
        )
        .unwrap();
    let id = reader
        .last_recording
        .as_ref()
        .expect("child recording")
        .recording_id;
    reader.release();
    let publications = Arc::new(Publications::new());
    let received = Arc::new(Mutex::new(Vec::new()));
    let producer = collector(Arc::clone(&publications), Arc::clone(&received));
    let mut server = VexCoreServer::new(
        fixture.config,
        GatewayAuthenticationKey::default(),
        producer,
        publications,
        true,
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    assert_eq!(server.recording_id, Some(id));
    assert_eq!(*received.lock().unwrap(), [1]);
    server.shutdown().unwrap();
}

#[test]
#[ignore = "needs fresh empty dedicated live Aeron archive/driver; set VEX_RECOVERY_AERON_DIR and VEX_RECOVERY_ARCHIVE_CHANNEL"]
fn c8_3_live_empty_legacy_subscription_is_removed() {
    let fixture = Fixture::new();
    let mut counter = Handler::leak(RecordingCounter);
    let existing = fixture
        .archive
        .list_recordings_for_uri(
            0,
            i32::MAX,
            &RECORDING_CHANNEL.into_c_string(),
            RECORDING_STREAM_ID,
            Some(&counter),
        )
        .unwrap();
    counter.release();
    assert_eq!(
        existing, 0,
        "this regression requires a fresh empty archive"
    );
    let (id, _sub, publication) = fixture.recording(0);
    publication.close::<AeronNotificationLogger>(None).unwrap();
    assert_eq!(
        VexCoreServer::wait_for_recording_stop(&fixture.archive, id).unwrap(),
        0
    );
    let publications = Arc::new(Publications::new());
    let producer = collector(Arc::clone(&publications), Arc::new(Mutex::new(Vec::new())));
    let mut server = VexCoreServer::new(
        fixture.config,
        GatewayAuthenticationKey::default(),
        producer,
        publications,
        true,
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    assert_ne!(server.recording_id, Some(id));
    server.shutdown().unwrap();
}

#[cfg(unix)]
#[test]
#[ignore = "needs dedicated live Aeron driver and separate archive process; set VEX_RECOVERY_AERON_DIR, VEX_RECOVERY_ARCHIVE_CHANNEL, VEX_RECOVERY_ARCHIVE_PID; test pauses archive with SIGSTOP"]
fn c7_1_live_queued_shutdown_lagging_archive_survives_recovery() {
    struct ResumeArchive(String);
    impl Drop for ResumeArchive {
        fn drop(&mut self) {
            std::process::Command::new("kill")
                .args(["-CONT", &self.0])
                .status()
                .unwrap();
        }
    }
    let fixture = Fixture::new();
    let publications = Arc::new(Publications::new());
    let journal = Arc::clone(&publications);
    let ack = Arc::clone(&publications);
    let (release, gate) = mpsc::channel();
    let mut producer = build_multi_producer(64, OrderCommand::default, BusySpin)
        .handle_events_with(move |cell, sequence, _| {
            gate.recv_timeout(DRAIN_TIMEOUT).unwrap();
            // SAFETY: Sole consumer reads the current ring slot.
            journal.publish_to_archive(unsafe { &*cell.get() });
            ack.completed(sequence);
        })
        .build();
    let mut server = VexCoreServer::new(
        fixture.config.clone(),
        GatewayAuthenticationKey::default(),
        producer.clone(),
        Arc::clone(&publications),
        false,
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    let id = server.recording_id.unwrap();
    for order_id in 1..=3 {
        publications.submitted(
            producer
                .try_publish(|cmd| {
                    *cmd = OrderCommand {
                        order_id,
                        ..OrderCommand::default()
                    };
                })
                .unwrap(),
        );
    }
    let pid = std::env::var("VEX_RECOVERY_ARCHIVE_PID").expect("dedicated archive PID required");
    assert!(pid.parse::<u32>().is_ok());
    assert!(
        std::process::Command::new("kill")
            .args(["-STOP", &pid])
            .status()
            .unwrap()
            .success()
    );
    let resume = ResumeArchive(pid);
    let worker = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(100));
        drop(resume);
        let observer = Fixture::new();
        // With journaling still gated, shutdown must not have stopped recording.
        // A separate control session observes this after the archive resumes.
        let still_active = observer.archive.get_stop_position(id).unwrap() == -1;
        for _ in 0..3 {
            release.send(()).unwrap();
        }
        assert!(
            still_active,
            "shutdown stopped recording before accepted work drained"
        );
    });
    server.shutdown().unwrap();
    worker.join().unwrap();
    assert_eq!(
        fixture.archive.get_max_recorded_position(id).unwrap(),
        3 * FRAMESIZE
    );
    drop(server);
    drop(producer);
    let recovered = Arc::new(Mutex::new(Vec::new()));
    let replay_publications = Arc::new(Publications::new());
    let replay_producer = collector(Arc::clone(&replay_publications), Arc::clone(&recovered));
    assert_eq!(
        fixture
            .replay(replay_publications, replay_producer.clone())
            .recording_id,
        id
    );
    assert_eq!(*recovered.lock().unwrap(), [1, 2, 3]);
}
