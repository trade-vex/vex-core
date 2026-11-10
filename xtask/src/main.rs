use clap::{Parser, Subcommand, ValueEnum};
use duct::cmd;
use serde::Deserialize;
use std::{
    collections::HashSet,
    env, fs,
    path::{Path, PathBuf},
    process::Child,
    time::Instant,
};
use thiserror::Error;
use tracing::{error, info};
use tracing_subscriber::FmtSubscriber;
use vex_config::GatewayNetworkingConfig;

use xtask::kafka_topics::{self, KafkaTopics};
use xtask::scenarios;
use xtask::test_framework::TestContext;
use xtask::test_framework::types::{ScenarioResult, TestConfig, TestSuiteResult};

/// delay in network emulation in teste2e (high-latency scenario)
const NETWORK_DELAY_MS: u64 = 100;
/// packet loss percentage in network emulation in teste2e (packet-loss scenario)
const PACKET_LOSS_PERCENT: u32 = 10;
/// default number of messages each client sends in teste2e
const DEFAULT_MSG_COUNT_PER_CLIENT: usize = 1000;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Build the Docker images for the server and client.
    BuildDocker,

    /// Unified test command with subcommands for all test types.
    Test {
        #[command(subcommand)]
        command: TestCommands,
    },

    /// The Kafka topics and Schema Registry subjects that deploy/kafka/topics.toml declares.
    Kafka {
        #[command(subcommand)]
        command: KafkaCommands,
    },
}

#[derive(Subcommand, Debug)]
enum KafkaCommands {
    /// Create the missing topics, set every topic's configuration and every subject's
    /// compatibility to what deploy/kafka/topics.toml declares. Applying it twice changes nothing.
    Apply {
        /// Kafka bootstrap servers
        #[arg(long)]
        brokers: String,

        /// Schema Registry URL
        #[arg(long)]
        schema_registry: String,

        /// A development or test cluster of one broker: replication factor and
        /// min.insync.replicas 1. The core refuses such event topics in production.
        #[arg(long)]
        single_broker: bool,
    },
}

#[derive(Subcommand, Debug)]
enum TestCommands {
    /// Docker-based network E2E tests
    E2e {
        /// Test scenario to run (basic-connectivity, high-latency, packet-loss)
        #[arg(short, long, default_value = "basic-connectivity")]
        scenario: String,

        /// Number of clients (default: 1)
        #[arg(short, long, default_value_t = 1)]
        clients: u32,

        /// Run all scenarios sequentially
        #[arg(long)]
        all: bool,
    },

    /// Integration tests (all, balance, gtc, ioc, fok, cancellation)
    Integration {
        /// Test suite to run
        #[arg(default_value = "all")]
        suite: IntegrationSuite,

        /// Verbose logging (debug level)
        #[arg(short, long)]
        verbose: bool,

        /// Stop on first failure
        #[arg(short, long)]
        fail_fast: bool,
    },

    /// The core's outbox: re-publication after a kill or a shutdown past its grace, the wait
    /// for Kafka at start, and the startup topic check (Docker, with an Aeron archive)
    Outbox,

    /// Performance benchmarks
    Benchmark {
        /// Number of clients (default: 1)
        #[arg(short, long, default_value_t = 1)]
        clients: u32,
    },

    /// Run cargo test for xtask unit tests
    Unit {
        /// Filter pattern for tests
        #[arg(short, long)]
        filter: Option<String>,
    },
}

#[derive(ValueEnum, Clone, Debug, Default)]
enum IntegrationSuite {
    #[default]
    All,
    Balance,
    Gtc,
    Ioc,
    Fok,
    Cancellation,
}

#[derive(Error, Debug)]
enum XTaskError {
    #[error("Failed to execute command: {0}")]
    CommandExecution(#[from] std::io::Error),

    #[error("Environment error: {0}")]
    Environment(#[from] std::env::VarError),

    #[error("An unexpected error occurred: {0}")]
    Unexpected(String),

    #[error("Test Failed: {0}")]
    Testing(#[from] TestingError),

    #[error("JSON parsing error: {0}")]
    JsonParse(#[from] serde_json::Error),

    #[error("Integration test error: {0}")]
    IntegrationTest(String),
}

#[derive(Error, Debug)]
enum TestingError {
    #[error("Test failed: {0}")]
    TestFailed(String),
}

#[tokio::main]
async fn main() -> Result<(), XTaskError> {
    let cli = Cli::parse();
    let project_root = env::current_dir()?;

    match cli.command {
        Commands::BuildDocker => build_docker(&project_root),

        Commands::Test { command } => match command {
            TestCommands::E2e {
                scenario,
                clients,
                all,
            } => {
                if all {
                    run_all_e2e_scenarios(project_root.into_boxed_path()).await
                } else {
                    run_correctness_task(project_root.into_boxed_path(), &scenario, clients).await
                }
            }
            TestCommands::Integration {
                suite,
                verbose,
                fail_fast,
            } => run_integration_tests(&project_root, suite, verbose, fail_fast).await,
            TestCommands::Outbox => run_outbox_tests(&project_root).await,
            TestCommands::Benchmark { clients } => {
                run_benchmark(project_root.into_boxed_path(), clients).await
            }
            TestCommands::Unit { filter } => run_unit_tests(&project_root, filter),
        },

        Commands::Kafka {
            command:
                KafkaCommands::Apply {
                    brokers,
                    schema_registry,
                    single_broker,
                },
        } => apply_topics(&brokers, &schema_registry, single_broker).await,
    }
}

/// Applies deploy/kafka/topics.toml to the cluster at `brokers` and the registry at
/// `schema_registry`.
async fn apply_topics(
    brokers: &str,
    schema_registry: &str,
    single_broker: bool,
) -> Result<(), XTaskError> {
    let file = KafkaTopics::load().map_err(XTaskError::Unexpected)?;
    kafka_topics::apply(
        &file.topics(single_broker),
        &file.subjects(),
        brokers,
        schema_registry,
    )
    .await
    .map_err(XTaskError::Unexpected)
}

/// Run all E2E test scenarios sequentially
async fn run_all_e2e_scenarios(project_root: Box<Path>) -> Result<(), XTaskError> {
    let scenarios = ["basic-connectivity", "high-latency", "packet-loss"];
    let mut failed_scenarios = Vec::new();

    for scenario in &scenarios {
        println!("\n========================================");
        println!("Running E2E scenario: {}", scenario);
        println!("========================================\n");

        if let Err(e) = run_correctness_task(project_root.clone(), scenario, 1).await {
            eprintln!("Scenario '{}' FAILED: {}", scenario, e);
            failed_scenarios.push(scenario.to_string());
        }
    }

    if failed_scenarios.is_empty() {
        println!("\n========================================");
        println!("All E2E scenarios PASSED!");
        println!("========================================\n");
        Ok(())
    } else {
        Err(XTaskError::Testing(TestingError::TestFailed(format!(
            "Failed scenarios: {}",
            failed_scenarios.join(", ")
        ))))
    }
}

/// Docker server IP from the docker-compose vex-testing-net subnet
const DOCKER_SERVER_IP: &str = "172.28.1.10";

/// The stack's Kafka listener and Schema Registry, as the host reaches them
/// (xtask/tests/docker-compose.yml).
const STACK_BROKERS: &str = "localhost:19092";
const STACK_SCHEMA_REGISTRY: &str = "http://localhost:18081";

/// The host-side Aeron media driver the integration test client connects through
///
/// It runs in the Aeron directory the test client uses, which it owns for the length of one run.
/// Dropping it stops the driver and deletes that directory whenever xtask returns or unwinds. A
/// signal that ends xtask (Ctrl-C, SIGKILL) skips Drop and can leave the directory behind, which is
/// why starting empties it first. A killed driver leaves its CnC file there with a recent
/// heartbeat: reusing the directory makes the next driver exit at once ("active driver detected")
/// while the client maps the stale file and fails with "no driver heartbeat".
struct HostMediaDriver {
    process: Child,
    dir: PathBuf,
}

impl HostMediaDriver {
    fn start(root: &Path, timeout: std::time::Duration) -> Result<Self, XTaskError> {
        let aeronmd_path = root.join("xtask/tests/bin/aeronmd");
        let lib_path = root.join("xtask/tests/bin/lib");

        if !aeronmd_path.exists() {
            return Err(XTaskError::Unexpected(format!(
                "aeronmd binary not found at {:?}. Run 'cargo xtask build-docker' first.",
                aeronmd_path
            )));
        }

        let dir = PathBuf::from(GatewayNetworkingConfig::test_defaults().context_dir);
        remove_dir_if_present(&dir).map_err(|e| {
            XTaskError::Unexpected(format!("Failed to clear {}: {e}", dir.display()))
        })?;

        println!(
            "Starting host-side Aeron media driver in {}...",
            dir.display()
        );
        let process = std::process::Command::new(&aeronmd_path)
            .env("AERON_DIR", &dir)
            .env(
                "LD_LIBRARY_PATH",
                format!(
                    "{}:{}",
                    lib_path.display(),
                    env::var("LD_LIBRARY_PATH").unwrap_or_default()
                ),
            )
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|e| XTaskError::Unexpected(format!("Failed to start aeronmd: {}", e)))?;

        // From here on, dropping the driver on an error stops it and removes its directory.
        let mut driver = Self { process, dir };
        driver.wait_for_cnc_file(timeout)?;
        Ok(driver)
    }

    /// Wait for the driver to create its CnC file, failing early if the driver exits
    fn wait_for_cnc_file(&mut self, timeout: std::time::Duration) -> Result<(), XTaskError> {
        let cnc_path = self.dir.join("cnc.dat");
        let start = Instant::now();

        while !cnc_path.exists() {
            if let Some(status) = self.process.try_wait()? {
                return Err(XTaskError::Unexpected(format!(
                    "aeronmd exited ({status}) before creating {}",
                    cnc_path.display()
                )));
            }
            if start.elapsed() > timeout {
                return Err(XTaskError::Unexpected(format!(
                    "Timed out waiting for Aeron CnC file at {}",
                    cnc_path.display()
                )));
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        println!("Aeron media driver ready (CnC file created).");
        Ok(())
    }
}

impl Drop for HostMediaDriver {
    fn drop(&mut self) {
        println!("Stopping host-side Aeron media driver...");
        if let Err(e) = self.process.kill().and_then(|()| self.process.wait()) {
            eprintln!("Warning: Failed to stop aeronmd: {e}");
        }
        if let Err(e) = remove_dir_if_present(&self.dir) {
            eprintln!("Warning: Failed to remove {}: {e}", self.dir.display());
        }
    }
}

fn remove_dir_if_present(dir: &Path) -> std::io::Result<()> {
    match fs::remove_dir_all(dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

/// Run integration tests directly (inlined from run_test_suite binary)
async fn run_integration_tests(
    root: &Path,
    suite: IntegrationSuite,
    verbose: bool,
    fail_fast: bool,
) -> Result<(), XTaskError> {
    // Initialize logging
    let subscriber = if verbose {
        FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .finish()
    } else {
        FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .finish()
    };

    // Try to set the subscriber, but ignore if already set (e.g., in tests)
    let _ = tracing::subscriber::set_global_default(subscriber);

    info!("Starting VEX-CORE Integration Test Suite");
    info!("========================================");

    // Start the host-side Aeron media driver for the test client. It is stopped and its directory
    // removed when this function returns, whichever way it returns; after a signal that skips
    // that, the next start empties the directory instead.
    let _media_driver = HostMediaDriver::start(root, std::time::Duration::from_secs(10))?;

    // Start redpanda, its topics, the archive and vex-server. The stack is torn down when this
    // function returns, a failed start included.
    println!("Starting infrastructure services...");
    let compose = DockerComposeEnv::start(root, &["vex-server"]).await?;

    println!("Waiting for services to stabilize...");
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    // Create test context with Docker server address
    let config = TestConfig {
        core_address: DOCKER_SERVER_IP.to_string(),
        ..Default::default()
    };

    let mut ctx = match TestContext::with_config(config).await {
        Ok(ctx) => ctx,
        Err(e) => {
            error!("Failed to initialize test context: {}", e);
            return Err(XTaskError::IntegrationTest(format!(
                "Failed to initialize test context: {}",
                e
            )));
        }
    };

    let mut suite_result = TestSuiteResult::new();
    let start = Instant::now();

    let test_result = run_suite_tests(&mut ctx, &suite).await;

    match test_result {
        Ok(results) => {
            for result in results {
                suite_result.add_result(result);
            }
        }
        Err(e) => {
            error!("Test suite failed: {}", e);
            if fail_fast {
                ctx.cleanup();
                return Err(XTaskError::IntegrationTest(e));
            }
        }
    }

    suite_result.duration = start.elapsed();

    // Cleanup
    ctx.cleanup();
    drop(compose);

    report(&suite_result)
}

/// The outbox scenarios, on a stack of their own: they pause the broker, and kill and restart the
/// core.
async fn run_outbox_tests(root: &Path) -> Result<(), XTaskError> {
    let _ = tracing::subscriber::set_global_default(
        FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .finish(),
    );
    let _media_driver = HostMediaDriver::start(root, std::time::Duration::from_secs(10))?;
    println!("Starting infrastructure services...");
    let compose = DockerComposeEnv::start(root, &["vex-server"]).await?;

    let config = TestConfig {
        core_address: DOCKER_SERVER_IP.to_string(),
        kafka_brokers: STACK_BROKERS.to_string(),
        ..Default::default()
    };
    let start = Instant::now();
    let results = scenarios::outbox::run_all(config, STACK_SCHEMA_REGISTRY)
        .await
        .map_err(|e| XTaskError::IntegrationTest(format!("Outbox suite failed: {e}")))?;
    drop(compose);

    let mut suite_result = TestSuiteResult::new();
    for result in results {
        suite_result.add_result(result);
    }
    suite_result.duration = start.elapsed();
    report(&suite_result)
}

/// Prints a suite's summary; an error if any scenario failed.
fn report(suite_result: &TestSuiteResult) -> Result<(), XTaskError> {
    // Print summary
    info!("");
    info!("========================================");
    info!("Test Suite Summary");
    info!("========================================");
    info!("Total scenarios:  {}", suite_result.total);
    info!("Passed:          {} ✓", suite_result.passed);
    info!("Failed:          {} ✗", suite_result.failed);
    info!("Duration:        {:?}", suite_result.duration);
    info!("");

    // Print individual results
    for result in &suite_result.scenarios {
        if result.success {
            info!("  ✓ {} ({:?})", result.name, result.duration);
        } else {
            error!("  ✗ {} ({:?})", result.name, result.duration);
            if let Some(ref err) = result.error {
                error!("    Error: {}", err);
            }
        }
    }

    info!("========================================");

    // Return appropriate result
    if suite_result.is_success() {
        info!("All tests passed! ✓");
        Ok(())
    } else {
        error!("Some tests failed! ✗");
        Err(XTaskError::Testing(TestingError::TestFailed(format!(
            "{} of {} tests failed",
            suite_result.failed, suite_result.total
        ))))
    }
}

async fn run_suite_tests(
    ctx: &mut TestContext,
    suite: &IntegrationSuite,
) -> Result<Vec<ScenarioResult>, String> {
    match suite {
        IntegrationSuite::All => {
            info!("Running Comprehensive Integration Test Suite");
            info!("(All order types + edge cases in single continuous state)");
            info!("");
            scenarios::comprehensive::run_all(ctx)
                .await
                .map_err(|e| format!("Comprehensive integration test failed: {}", e))
        }
        IntegrationSuite::Balance => {
            info!("Running Balance Management tests");
            scenarios::balance::run_all(ctx)
                .await
                .map_err(|e| format!("Balance tests failed: {}", e))
        }
        IntegrationSuite::Gtc => {
            info!("Running GTC Order tests");
            scenarios::gtc::run_all(ctx)
                .await
                .map_err(|e| format!("GTC tests failed: {}", e))
        }
        IntegrationSuite::Ioc => {
            info!("Running IOC Order tests");
            scenarios::ioc::run_all(ctx)
                .await
                .map_err(|e| format!("IOC tests failed: {}", e))
        }
        IntegrationSuite::Fok => {
            info!("Running FOK Order tests");
            scenarios::fok::run_all(ctx)
                .await
                .map_err(|e| format!("FOK tests failed: {}", e))
        }
        IntegrationSuite::Cancellation => {
            info!("Running Cancellation tests");
            scenarios::cancellation::run_all(ctx)
                .await
                .map_err(|e| format!("Cancellation tests failed: {}", e))
        }
    }
}

/// Run xtask unit tests
fn run_unit_tests(root: &Path, filter: Option<String>) -> Result<(), XTaskError> {
    println!("Running xtask unit tests...");

    let mut args = vec!["test", "--package", "xtask"];

    if let Some(ref f) = filter {
        args.push("--");
        args.push(f);
    }

    cmd("cargo", &args).dir(root).run()?;

    println!("Unit tests completed successfully.");
    Ok(())
}

/// A docker-compose stack in `root`, torn down (`docker compose down -v`) when dropped
///
/// Every constructor creates the guard before `docker compose up` runs, so a start that fails part
/// of the way through is torn down too. The logs of every container are saved before the teardown.
struct DockerComposeEnv {
    root: Box<Path>,
}
impl DockerComposeEnv {
    /// Brings up the broker, applies deploy/kafka/topics.toml to it as to any environment (the
    /// broker creates no topic by itself), then brings up `args` (services, `--scale`, ...),
    /// which the Aeron archive and vex-server are among, or are brought up with.
    async fn start(project_root: &Path, args: &[&str]) -> Result<Self, XTaskError> {
        let env = Self {
            root: project_root.join("xtask/tests").into_boxed_path(),
        };
        // Created here, or the containers' bind mount creates it owned by root, and the logs
        // cannot be saved into it.
        fs::create_dir_all(env.root.join("test-results/logs"))?;
        env.compose_up(&["--wait", "redpanda"])?;
        // The Schema Registry can answer a moment after the broker reports healthy.
        let mut attempts = 0;
        while let Err(e) = apply_topics(STACK_BROKERS, STACK_SCHEMA_REGISTRY, true).await {
            attempts += 1;
            if attempts == 30 {
                return Err(e);
            }
            println!("Applying the Kafka topics failed, retrying: {e}");
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
        env.compose_up(args)?;
        Ok(env)
    }

    /// Run `docker compose up -d` with `args`
    fn compose_up(&self, args: &[&str]) -> Result<(), XTaskError> {
        cmd("docker", ["compose", "up", "-d"].iter().chain(args))
            .dir(self.root.as_ref())
            .run()?;
        Ok(())
    }

    /// Start the e2e stack with `clients` client containers and wait for vex-server to be healthy
    async fn new(project_root: &Path, clients: u32) -> Result<Self, XTaskError> {
        let root = project_root.join("xtask/tests");

        println!("Bringing up docker-compose environment...");
        let env = Self::start(project_root, &["--scale", &format!("vex-client={clients}")]).await?;

        // Wait for vex-server to be healthy (has healthcheck in docker-compose.yml)
        println!("Waiting for vex-server to be healthy...");
        let start = Instant::now();
        let timeout = std::time::Duration::from_secs(120);
        loop {
            if start.elapsed() > timeout {
                // Print server logs for debugging before failing
                eprintln!("=== vex-server logs ===");
                if let Ok(logs) = cmd!("docker", "logs", "vex-server")
                    .dir(root.clone())
                    .read()
                {
                    eprintln!("{}", logs);
                }
                return Err(XTaskError::Unexpected(
                    "Timed out waiting for vex-server to become healthy".to_string(),
                ));
            }

            if let Ok(output) = cmd!(
                "docker",
                "inspect",
                "--format",
                "{{.State.Health.Status}}",
                "vex-server"
            )
            .dir(root.clone())
            .read()
            {
                let status = output.trim();
                if status == "healthy" {
                    println!("vex-server is healthy.");
                    break;
                }
                if status == "unhealthy" {
                    eprintln!("=== vex-server logs ===");
                    if let Ok(logs) = cmd!("docker", "logs", "vex-server")
                        .dir(root.clone())
                        .read()
                    {
                        eprintln!("{}", logs);
                    }
                    return Err(XTaskError::Unexpected(
                        "vex-server became unhealthy".to_string(),
                    ));
                }
            }
            std::thread::sleep(std::time::Duration::from_secs(2));
        }

        // Additional stabilization for Aeron subscriptions to be fully registered
        println!("Waiting for Aeron subscriptions to stabilize...");
        std::thread::sleep(std::time::Duration::from_secs(5));
        Ok(env)
    }
}

impl Drop for DockerComposeEnv {
    fn drop(&mut self) {
        let logs_dir = self.root.join("test-results/logs");
        println!("Saving docker-compose logs...");
        let containers = cmd!("docker", "compose", "ps", "--all", "--format", "json")
            .dir(self.root.as_ref())
            .read()
            .unwrap_or_default();
        // NDJSON, one container per line
        for container in containers
            .lines()
            .filter_map(|line| serde_json::from_str::<ComposeContainer>(line).ok())
        {
            let log_path = logs_dir.join(format!("{}.log", container.name));
            match cmd!("docker", "logs", &container.name)
                .stderr_to_stdout()
                .read()
            {
                Ok(output) => {
                    if let Err(e) = fs::write(&log_path, output) {
                        eprintln!("Failed to write logs for {}: {e}", container.name);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to get logs for {}: {e}", container.name);
                }
            }
        }

        println!("Tearing down docker-compose environment...");
        if let Err(e) = cmd!("docker", "compose", "down", "-v")
            .dir(self.root.as_ref())
            .run()
        {
            eprintln!("Failed to tear down docker environment: {e}");
        }
    }
}

#[derive(Deserialize, Debug)]
struct ComposeContainer {
    #[serde(rename = "Name")]
    name: String,
}

// Helper function to apply network conditions
fn apply_scenario_conditions(root: &Path, scenario: &str, clients: u32) -> Result<(), XTaskError> {
    if scenario == "basic-connectivity" {
        return Ok(()); // No network conditions to apply for basic connectivity
    }
    for i in 1..=clients {
        match scenario {
            "high-latency" => {
                println!("Applying {NETWORK_DELAY_MS}ms latency...");
                // Apply latency to traffic leaving the client
                cmd!(
                    "docker",
                    "compose",
                    "exec",
                    "--index",
                    i.to_string(),
                    "-T",
                    "vex-client",
                    "tc",
                    "qdisc",
                    "replace",
                    "dev",
                    "eth0",
                    "root",
                    "netem",
                    "delay",
                    format!("{NETWORK_DELAY_MS}ms")
                )
                .dir(root)
                .run()?;
                // Apply latency to traffic leaving the server
                cmd!(
                    "docker",
                    "compose",
                    "exec",
                    "-T",
                    "vex-server",
                    "tc",
                    "qdisc",
                    "replace",
                    "dev",
                    "eth0",
                    "root",
                    "netem",
                    "delay",
                    format!("{NETWORK_DELAY_MS}ms")
                )
                .dir(root)
                .run()?;
            }
            "packet-loss" => {
                println!("Applying {PACKET_LOSS_PERCENT}% packet loss...");
                cmd!(
                    "docker",
                    "compose",
                    "exec",
                    "--index",
                    i.to_string(),
                    "-T",
                    "vex-client",
                    "tc",
                    "qdisc",
                    "replace",
                    "dev",
                    "eth0",
                    "root",
                    "netem",
                    "loss",
                    format!("{PACKET_LOSS_PERCENT}%")
                )
                .dir(root)
                .run()?;
            }
            _ => Err(XTaskError::Unexpected(format!(
                "Unknown scenario: {scenario}"
            )))?,
        }
    }
    Ok(())
}

fn build_docker(root: &Path) -> Result<(), XTaskError> {
    let dir = root.join("xtask/tests");
    // Export the media driver first, while nothing is executing it. The exporter copies
    // aeronmd into ./bin, which is the path `HostMediaDriver::start` later runs, so
    // exporting afterwards fails with ETXTBSY.
    println!("Exporting the Aeron media driver...");
    cmd!(
        "docker",
        "compose",
        "run",
        "--rm",
        "--build",
        "--no-deps",
        "media-driver"
    )
    .dir(&dir)
    .run()?;
    println!("Building all Docker services...");
    cmd!(
        "docker",
        "compose",
        "build",
        "aeron-archive",
        "vex-server",
        "vex-client"
    )
    .dir(&dir)
    .run()?;
    println!("Docker images built successfully.");
    Ok(())
}

async fn run_correctness_task(
    project_root: Box<Path>,
    scenario: &str,
    clients: u32,
) -> Result<(), XTaskError> {
    let test_results_dir = project_root.join("xtask/tests/test-results");
    // Clear up/delete existing contents in the test-results directory
    if test_results_dir.exists() {
        println!("Cleaning up existing test results...");
        fs::remove_dir_all(&test_results_dir)?;
    }
    fs::create_dir_all(&test_results_dir)?;

    // Wrap docker-compose commands in a struct with a Drop implementation for cleanup
    let _env = DockerComposeEnv::new(&project_root, clients).await?;

    // --- Scenario-specific setup ---
    apply_scenario_conditions(&project_root.join("xtask/tests"), scenario, clients)?;

    let msg_count_per_client = DEFAULT_MSG_COUNT_PER_CLIENT;
    let total_msg_count = msg_count_per_client * clients as usize;

    println!("Executing {clients} clients to send {msg_count_per_client} messages each...");
    let mut threads = Vec::new();
    for i in 1..=clients {
        let project_root = project_root.clone();
        threads.push(std::thread::spawn(move || {
            cmd!(
                "docker",
                "compose",
                "exec",
                "--index",
                (i).to_string(),
                "-T",
                "vex-client",
                "/usr/local/bin/test_client",
                "--client-id",
                (i - 1).to_string(),
                "correctness",
                "--count",
                msg_count_per_client.to_string(),
            )
            .dir(project_root.join("xtask/tests"))
            .run()
        }));
    }

    for handle in threads {
        handle.join().unwrap()?;
    }
    println!("All clients finished execution.");

    println!("Client finished execution.");

    // --- Verify Results ---
    println!("Verifying results...");

    let received_ids: HashSet<u64> = read_received_ids(&test_results_dir, clients)?
        .into_iter()
        .collect();

    // Assertion 1: Check for completeness
    if received_ids.len() != total_msg_count {
        return Err(XTaskError::Testing(TestingError::TestFailed(format!(
            "Expected {} messages, but received {}.",
            total_msg_count,
            received_ids.len()
        ))));
    }

    // Assertion 2: Check for duplicates/gaps
    for c in 0..clients as u64 {
        let id_offset = c * 1_000_000;
        for i in 0..msg_count_per_client as u64 {
            if !received_ids.contains(&(id_offset + i)) {
                return Err(XTaskError::Testing(TestingError::TestFailed(format!(
                    "Missing message ID: {i}"
                ))));
            }
        }
    }

    println!("Scenario '{scenario}' PASSED!");
    Ok(())
}

async fn run_benchmark(root: Box<Path>, clients: u32) -> Result<(), XTaskError> {
    let test_results_dir = root.join("xtask/tests/test-results");
    // Clear up/delete existing contents in the test-results directory
    if test_results_dir.exists() {
        println!("Cleaning up existing test results...");
        fs::remove_dir_all(&test_results_dir)?;
    }
    fs::create_dir_all(&test_results_dir)?;

    // Wrap docker-compose commands in a struct with a Drop implementation for cleanup
    let _env = DockerComposeEnv::new(&root, clients).await?;

    // Fixed: Use actual message count, not hardcoded duration
    let msg_count_per_client = 10000;
    let total_expected_messages = msg_count_per_client * clients;

    println!("Starting {clients} clients, each sending {msg_count_per_client} messages...");
    println!("Total expected messages: {total_expected_messages}");

    let mut threads = Vec::new();
    for i in 1..=clients {
        let project_root = root.clone();
        threads.push(std::thread::spawn(move || {
            let thread_start = Instant::now();
            let result = cmd!(
                "docker",
                "compose",
                "exec",
                "--index",
                (i).to_string(),
                "-T",
                "vex-client",
                "sh",
                "-c",
                &format!("/usr/local/bin/test_client --client-id {} latency --samples {} | tee /proc/1/fd/1",
                        i - 1, msg_count_per_client)
            )
            .dir(project_root.join("xtask/tests"))
            .run();

            (result, thread_start.elapsed())
        }));
    }

    // Wait for all clients to complete and collect timing info
    let mut client_durations = Vec::new();
    let mut failed_clients = 0;

    for (i, thread) in threads.into_iter().enumerate() {
        match thread.join() {
            Ok((result, duration)) => {
                client_durations.push(duration);
                if let Err(e) = result {
                    eprintln!("Client {} failed: {}", i + 1, e);
                    failed_clients += 1;
                }
            }
            Err(_) => {
                eprintln!("Client {} thread panicked", i + 1);
                failed_clients += 1;
            }
        }
    }

    println!("All clients finished execution.");

    // Read and analyze results. Each client writes every id at most once.
    let messages_received = read_received_ids(&test_results_dir, clients)?.len();

    let success_rate = (messages_received as f64 / total_expected_messages as f64) * 100.0;

    // Print comprehensive results
    println!("\n╔════════════════════════════════════════════╗");
    println!("║      THROUGHPUT BENCHMARK RESULTS          ║");
    println!("╠════════════════════════════════════════════╣");
    println!("║ Configuration:                             ║");
    println!("║   Clients:              {clients:>18} ║");
    println!("║   Messages per client:  {msg_count_per_client:>18} ║");
    println!("║   Expected total:       {total_expected_messages:>18} ║");
    println!("╠════════════════════════════════════════════╣");
    println!("║ Results:                                   ║");
    println!("║   Messages received:    {messages_received:>18} ║");
    println!("║   Success rate:         {success_rate:>17.2}% ║");
    println!("║   Failed clients:       {failed_clients:>18} ║");
    println!("╠════════════════════════════════════════════╣");
    println!("╚════════════════════════════════════════════╝");

    // Determine test status
    if messages_received == 0 {
        println!("\nBenchmark FAILED: No messages received");
        return Err(XTaskError::Unexpected("No messages received".to_string()));
    } else if success_rate < 100.0 {
        println!(
            "\nBenchmark completed with message loss: {:.2}%",
            100.0 - success_rate
        );
    } else {
        println!("\n Benchmark completed successfully!");
    }
    Ok(())
}

/// Read the ids each e2e or benchmark client wrote to `received_ids-<client id>.txt`
/// (`write_received_ids` in `test_client`)
fn read_received_ids(results_dir: &Path, clients: u32) -> Result<Vec<u64>, XTaskError> {
    let mut ids = Vec::new();
    for client_id in 0..clients {
        let path = results_dir.join(format!("received_ids-{client_id}.txt"));
        let content = fs::read_to_string(&path).map_err(|e| {
            XTaskError::Unexpected(format!(
                "Client {client_id} wrote no results to {}: {e}",
                path.display()
            ))
        })?;
        for line in content.lines() {
            ids.push(line.parse().map_err(|e| {
                XTaskError::Unexpected(format!("Invalid id {line:?} in {}: {e}", path.display()))
            })?);
        }
    }
    Ok(ids)
}
