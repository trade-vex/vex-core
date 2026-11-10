//! Live regressions: use a dedicated, disposable archive/driver; run with --test-threads=1.
//! VEX_RECOVERY_AERON_DIR and VEX_RECOVERY_ARCHIVE_CHANNEL select that instance.
//! C7-1 and C8-3's unrecorded-commands test additionally need VEX_RECOVERY_ARCHIVE_PID (a separate
//! Unix archive process); the latter needs that archive in shared threading mode to fail without
//! its fix.

use super::*;
use common::{FRAMESIZE, ORDERCOMMANDSIZE, encode_order_command};
use disruptor::{BusySpin, Producer, build_multi_producer};
use rusteron_archive::{AeronPublication, AeronReservedValueSupplierLogger, RecordingPos};
use std::sync::{Mutex, mpsc};

/// Set in a child process that a crash test runs to crash (see [`run_crashing_child`]).
const CRASH_CHILD: &str = "VEX_RECOVERY_CRASH_CHILD";

struct Fixture {
    config: CoreNetworkingConfig,
    aeron: Aeron,
    archive: AeronArchive,
    release_timeout: Duration,
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
        let release_timeout = VexCoreServer::journal_release_timeout(&config.context_dir).unwrap();
        Self {
            config,
            aeron,
            archive,
            release_timeout,
        }
    }

    /// Records `commands` commands whose order ids and timestamps are 1, 2, 3, ...
    fn recording(&self, commands: usize) -> (i64, i64, AeronPublication) {
        let channel = "aeron:ipc?term-length=65536".into_c_string();
        let subscription = self
            .archive
            .start_recording(
                &channel,
                RECORDING_STREAM_ID,
                SourceLocation::AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
                true,
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
            journal_command(&publication, id as u64 + 1);
        }
        let id = recording_id.unwrap();
        wait_until(DRAIN_TIMEOUT, || {
            Ok(self.archive.get_recording_position(id)? >= publication.position())
        })
        .unwrap();
        (id, subscription, publication)
    }

    /// Ends a [`Fixture::recording`] as a clean shutdown does: stops it and closes its publication.
    fn stop(&self, subscription_id: i64, publication: AeronPublication) {
        self.archive
            .stop_recording_subscription(subscription_id)
            .unwrap();
        publication.close::<AeronNotificationLogger>(None).unwrap();
    }

    fn holders(&self) -> JournalHolders {
        VexCoreServer::journal_holders(&self.aeron, &self.archive).unwrap()
    }

    /// Waits until nothing holds the journal, as a start does.
    fn await_release(&self) {
        await_journal_release(self.release_timeout, &AtomicBool::new(false), || {
            VexCoreServer::journal_holders(&self.aeron, &self.archive)
        })
        .unwrap();
    }

    /// Starts a core on this journal, and returns it with the order ids its start replayed.
    fn start_core(&self) -> (VexCoreServer, Vec<u64>) {
        let publications = Arc::new(Publications::new());
        let replayed = Arc::new(Mutex::new(Vec::new()));
        let producer = collector(Arc::clone(&publications), Arc::clone(&replayed));
        let server = VexCoreServer::new(
            self.config.clone(),
            GatewayAuthenticationKey::default(),
            producer,
            publications,
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        let replayed = replayed.lock().unwrap().clone();
        (server, replayed)
    }

    fn replay(
        &self,
        publications: Arc<Publications>,
        producer: MultiProducer<OrderCommand, SingleConsumerBarrier>,
    ) -> ExtendedRecordingDescriptor {
        let recordings = VexCoreServer::list_recordings(
            &self.aeron,
            &self.archive,
            self.release_timeout,
            &AtomicBool::new(false),
        )
        .unwrap();
        VexCoreServer::start_replay(
            &self.aeron,
            &self.archive,
            replay_lineage(&recordings).expect("a recording to replay"),
            producer,
            Arc::new(AtomicBool::new(false)),
            publications,
        )
        .unwrap()
    }
}

/// Journals a command whose order id and timestamp are `order_id`.
fn journal_command(publication: &AeronPublication, order_id: u64) {
    let command = OrderCommand {
        order_id,
        timestamp: order_id,
        ..OrderCommand::default()
    };
    let mut bytes = [0; ORDERCOMMANDSIZE];
    encode_order_command(&command, &mut bytes).unwrap();
    wait_until(DRAIN_TIMEOUT, || {
        Ok(publication.offer::<AeronReservedValueSupplierLogger>(&bytes, None) >= 0)
    })
    .unwrap();
}

/// Runs `test`, this binary's test of that name, in a child process with [`CRASH_CHILD`] set, and
/// returns whether it succeeded. The child's part of the test ends in [`crash`].
fn run_crashing_child(test: &str) -> bool {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([test, "--ignored", "--exact"])
        .env(CRASH_CHILD, "1")
        .status()
        .unwrap()
        .success()
}

fn is_crash_child() -> bool {
    std::env::var_os(CRASH_CHILD).is_some()
}

/// Exits abruptly, as a killed process does: no Rust destructor runs, so the child neither stops
/// its recording nor closes its Aeron client, and the archive and media driver stay alive.
fn crash() -> ! {
    std::process::exit(0)
}

/// Sends `signal` (STOP or CONT) to the dedicated archive process.
fn signal_archive(pid: &str, signal: &str) {
    assert!(
        std::process::Command::new("kill")
            .args([&format!("-{signal}"), pid])
            .status()
            .unwrap()
            .success()
    );
}

/// The dedicated archive process's pid, which a test pauses.
fn archive_pid() -> String {
    let pid = std::env::var("VEX_RECOVERY_ARCHIVE_PID").expect("dedicated archive PID required");
    assert!(pid.parse::<u32>().is_ok());
    pid
}

/// Resumes the paused archive when dropped, whatever the test's outcome.
struct ResumeArchive(String);

impl Drop for ResumeArchive {
    fn drop(&mut self) {
        signal_archive(&self.0, "CONT");
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
    fixture.stop(subscription_id, publication);
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
fn live_every_start_replays_the_journal_and_reads_its_tail_from_the_last_term() {
    let fixture = Fixture::new();
    // The first term ends in padding and the last command opens the second.
    let count = (65_536 / FRAMESIZE + 1) as usize;
    let (id, subscription_id, publication) = fixture.recording(count);
    fixture.stop(subscription_id, publication);

    // Each start replays the recording and extends it; with nothing journaled in between, the
    // next start replays the same commands, and continues above the same tail.
    for _ in 0..2 {
        let publications = Arc::new(Publications::new());
        let replayed = Arc::new(Mutex::new(Vec::new()));
        let producer = collector(Arc::clone(&publications), Arc::clone(&replayed));
        let mut server = VexCoreServer::new(
            fixture.config.clone(),
            GatewayAuthenticationKey::default(),
            producer,
            Arc::clone(&publications),
            Arc::new(AtomicBool::new(false)),
        )
        .unwrap();
        assert_eq!(server.recording_id, Some(id));
        assert_eq!(publications.journal_tail(), count as u64);
        assert_eq!(replayed.lock().unwrap().len(), count);
        server.shutdown().unwrap();
    }
}

#[test]
#[ignore = "needs dedicated live Aeron driver/archive; set VEX_RECOVERY_AERON_DIR and VEX_RECOVERY_ARCHIVE_CHANNEL"]
fn c8_2_live_held_terminal_second_recovery_has_no_duplicate_journal_entries() {
    let fixture = Fixture::new();
    let (id, sub, publication) = fixture.recording(1);
    let original_stop = publication.position();
    fixture.stop(sub, publication);

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
    // Release the first core's journal publication, as its process's exit would: the second
    // recovery waits until no publication holds the journal.
    drop(publications);
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
fn c8_3_live_restart_after_a_crash_mid_recording_replays_and_extends_that_recording() {
    if is_crash_child() {
        let fixture = Fixture::new();
        let (_id, _sub, _publication) = fixture.recording(1);
        crash();
    }
    let fixture = Fixture::new();
    // Nothing else holds the journal, so what holds it after the crash is the child's.
    fixture.await_release();
    assert!(run_crashing_child(
        "server::recovery_tests::c8_3_live_restart_after_a_crash_mid_recording_replays_and_extends_that_recording"
    ));
    // The restart begins while the dead child holds the journal: the media driver holds its
    // publication until the client liveness timeout passes, and the archive still records it.
    let holders = fixture.holders();
    assert_eq!(holders.live_recordings.len(), 1, "{holders:?}");
    assert_eq!(holders.publications.len(), 1, "{holders:?}");
    let (mut server, replayed) = fixture.start_core();
    assert_eq!(server.recording_id, Some(holders.live_recordings[0]));
    assert_eq!(replayed, [1]);
    server.shutdown().unwrap();
}

#[test]
#[ignore = "needs dedicated live Aeron driver/archive surviving a child-process crash; set VEX_RECOVERY_AERON_DIR and VEX_RECOVERY_ARCHIVE_CHANNEL"]
fn c8_3_live_restart_after_a_crash_past_its_recording_stop_waits_for_its_publication() {
    if is_crash_child() {
        // Killed at the end of a graceful shutdown: the recording has stopped, at its final
        // position, but the process has not closed its publication yet.
        let fixture = Fixture::new();
        let (id, subscription_id, _publication) = fixture.recording(1);
        fixture
            .archive
            .stop_recording_subscription(subscription_id)
            .unwrap();
        wait_until(DRAIN_TIMEOUT, || {
            Ok(!is_live_recording(fixture.archive.get_stop_position(id)?))
        })
        .unwrap();
        crash();
    }
    let fixture = Fixture::new();
    fixture.await_release();
    assert!(run_crashing_child(
        "server::recovery_tests::c8_3_live_restart_after_a_crash_past_its_recording_stop_waits_for_its_publication"
    ));
    // Only the dead child's publication, which the media driver still holds, holds the journal.
    let holders = fixture.holders();
    assert!(holders.live_recordings.is_empty(), "{holders:?}");
    assert_eq!(holders.publications.len(), 1, "{holders:?}");
    let (mut server, replayed) = fixture.start_core();
    assert_eq!(replayed, [1]);
    server.shutdown().unwrap();
}

#[cfg(unix)]
#[test]
#[ignore = "needs dedicated live Aeron driver and separate archive process surviving a child-process crash; set VEX_RECOVERY_AERON_DIR, VEX_RECOVERY_ARCHIVE_CHANNEL, VEX_RECOVERY_ARCHIVE_PID; test pauses archive with SIGSTOP"]
fn c8_3_live_restart_replays_the_commands_a_crash_left_unrecorded() {
    let pid = archive_pid();
    if is_crash_child() {
        let fixture = Fixture::new();
        let (_id, _sub, publication) = fixture.recording(1);
        // With the archive paused, the next commands are journaled in the publication only.
        signal_archive(&pid, "STOP");
        wait_until(DRAIN_TIMEOUT, || {
            let state = std::process::Command::new("ps")
                .args(["-o", "stat=", "-p", &pid])
                .output()?
                .stdout;
            Ok(state.starts_with(b"T"))
        })
        .unwrap();
        journal_command(&publication, 2);
        journal_command(&publication, 3);
        crash();
    }
    // Connected before the child pauses the archive, so that the restart's first request reaches
    // the archive while it is paused, ahead of the recording of commands 2 and 3.
    let fixture = Fixture::new();
    fixture.await_release();
    let resume = ResumeArchive(pid);
    let child_succeeded = run_crashing_child(
        "server::recovery_tests::c8_3_live_restart_replays_the_commands_a_crash_left_unrecorded",
    );
    let resumer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(200));
        drop(resume);
    });
    let publications = Arc::new(Publications::new());
    let replayed = Arc::new(Mutex::new(Vec::new()));
    let producer = collector(Arc::clone(&publications), Arc::clone(&replayed));
    fixture.replay(publications, producer.clone());
    resumer.join().unwrap();
    assert!(child_succeeded);
    assert_eq!(*replayed.lock().unwrap(), [1, 2, 3]);
}

#[test]
#[ignore = "needs dedicated live Aeron driver/archive; set VEX_RECOVERY_AERON_DIR and VEX_RECOVERY_ARCHIVE_CHANNEL"]
fn c8_3_live_subscription_left_by_a_crash_before_the_publication_connected_is_stopped() {
    let fixture = Fixture::new();
    // What a process killed between starting its recording and its publication connecting
    // leaves behind: a recording subscription with no image, which auto_stop never removes.
    fixture
        .archive
        .start_recording(
            &RECORDING_CHANNEL.into_c_string(),
            RECORDING_STREAM_ID,
            SourceLocation::AERON_ARCHIVE_SOURCE_LOCATION_LOCAL,
            true,
        )
        .unwrap();
    let publications = Arc::new(Publications::new());
    let producer = collector(Arc::clone(&publications), Arc::new(Mutex::new(Vec::new())));
    let mut server = VexCoreServer::new(
        fixture.config,
        GatewayAuthenticationKey::default(),
        producer,
        publications,
        Arc::new(AtomicBool::new(false)),
    )
    .expect("restart must not fail with \"recording exists\"");
    server.shutdown().unwrap();
}

#[cfg(unix)]
#[test]
#[ignore = "needs dedicated live Aeron driver and separate archive process; set VEX_RECOVERY_AERON_DIR, VEX_RECOVERY_ARCHIVE_CHANNEL, VEX_RECOVERY_ARCHIVE_PID; test pauses archive with SIGSTOP"]
fn c7_1_live_queued_shutdown_lagging_archive_survives_recovery() {
    let fixture = Fixture::new();
    let publications = Arc::new(Publications::new());
    let journal = Arc::clone(&publications);
    let ack = Arc::clone(&publications);
    let (release, gate) = mpsc::channel();
    // The start replays whatever the archive already holds; only the live commands are held.
    let live = Arc::new(AtomicBool::new(false));
    let replayed = Arc::new(Mutex::new(Vec::new()));
    let (handler_live, handler_replayed) = (Arc::clone(&live), Arc::clone(&replayed));
    let mut producer = build_multi_producer(64, OrderCommand::default, BusySpin)
        .handle_events_with(move |cell, sequence, _| {
            // SAFETY: Sole consumer reads the current ring slot.
            let command = unsafe { &*cell.get() };
            if handler_live.load(Ordering::Acquire) {
                gate.recv_timeout(DRAIN_TIMEOUT).unwrap();
                journal.publish_to_archive(command);
            } else {
                handler_replayed.lock().unwrap().push(command.order_id);
            }
            ack.completed(sequence);
        })
        .build();
    let mut server = VexCoreServer::new(
        fixture.config.clone(),
        GatewayAuthenticationKey::default(),
        producer.clone(),
        Arc::clone(&publications),
        Arc::new(AtomicBool::new(false)),
    )
    .unwrap();
    live.store(true, Ordering::Release);
    let id = server.recording_id.unwrap();
    let extended_from = fixture.archive.get_max_recorded_position(id).unwrap();
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
    let pid = archive_pid();
    signal_archive(&pid, "STOP");
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
        extended_from + 3 * FRAMESIZE
    );
    // Release the core's journal publication, as its process's exit would: the recovery waits
    // until no publication holds the journal.
    drop(server);
    drop(producer);
    drop(publications);
    let recovered = Arc::new(Mutex::new(Vec::new()));
    let replay_publications = Arc::new(Publications::new());
    let replay_producer = collector(Arc::clone(&replay_publications), Arc::clone(&recovered));
    assert_eq!(
        fixture
            .replay(replay_publications, replay_producer.clone())
            .recording_id,
        id
    );
    let mut expected = replayed.lock().unwrap().clone();
    expected.extend([1, 2, 3]);
    assert_eq!(*recovered.lock().unwrap(), expected);
}
