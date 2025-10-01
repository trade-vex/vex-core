use clap::{Parser, Subcommand};
use duct::cmd;
use serde::Deserialize;
use std::{collections::HashSet, env, fs, path::Path, time::Instant};
use thiserror::Error;

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
    /// Run End-to-End tests using a specified scenario.
    TestE2e {
        #[arg(short, long, default_value = "basic-connectivity")]
        scenario: String,
        #[arg(short, long, default_value_t = 1)]
        clients: u32,
    },
    /// Run performance benchmarks.
    Benchmark {
        #[arg(short, long, default_value_t = 1)]
        clients: u32,
    },
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
}

#[derive(Error, Debug)]
enum TestingError {
    #[error("Test failed: {0}")]
    TestFailed(String),
}

fn main() -> Result<(), XTaskError> {
    let cli = Cli::parse();
    let project_root = env::current_dir()?;

    match cli.command {
        Commands::BuildDocker => build_docker(&project_root),
        Commands::TestE2e { scenario, clients } => {
            run_correctness_task(project_root.into_boxed_path(), &scenario, clients)
        }
        Commands::Benchmark { clients } => run_benchmark(project_root.into_boxed_path(), clients),
    }
}

// Helper struct for automatic cleanup
struct DockerComposeEnv {
    root: Box<Path>,
    containers: Vec<String>,
}
impl DockerComposeEnv {
    fn new(root: Box<Path>, clients: u32) -> Result<Self, XTaskError> {
        fs::create_dir_all(root.join("test-results"))?;

        println!("Bringing up docker-compose environment...");
        cmd!(
            "docker",
            "compose",
            "up",
            "-d",
            "--scale",
            format!("vex-client={clients}")
        )
        .dir(root.clone())
        .run()?;

        println!("Waiting for environment to stabilize...");
        std::thread::sleep(std::time::Duration::from_secs(5));

        let json_output = cmd!("docker", "compose", "ps", "--format", "json")
            .dir(root.clone())
            .read()?;

        // Parse NDJSON output
        let containers = json_output
            .lines()
            .filter_map(
                |line| match serde_json::from_str::<ComposeContainer>(line) {
                    Ok(c) if c.service == "vex-client" || c.service == "vex-server" => Some(c.name),
                    _ => None,
                },
            )
            .collect::<Vec<_>>();

        Ok(Self { root, containers })
    }
}

impl Drop for DockerComposeEnv {
    fn drop(&mut self) {
        println!("Saving docker-compose logs...");
        let logs_dir = self.root.join("test-results/logs");

        if let Err(e) = fs::create_dir_all(&logs_dir) {
            eprintln!("Failed to create logs dir: {e}");
        }

        for container in &self.containers {
            let log_path = logs_dir.join(format!("{container}.log"));
            match cmd!("docker", "logs", container).read() {
                Ok(output) => {
                    if let Err(e) = fs::write(&log_path, output) {
                        eprintln!("Failed to write logs for {container}: {e}");
                    }
                }
                Err(e) => {
                    eprintln!("Failed to get logs for {container}: {e}");
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
    #[serde(rename = "Service")]
    service: String,
}

// Helper function to apply network conditions
fn apply_scenario_conditions(scenario: &str, clients: u32) -> Result<(), XTaskError> {
    if scenario == "basic-connectivity" {
        return Ok(()); // No network conditions to apply for basic connectivity
    }
    for i in 1..=clients {
        match scenario {
            "high-latency" => {
                println!("Applying 100ms RTT latency...");
                // Apply 50ms latency to traffic leaving the client
                cmd!(
                    "docker",
                    "exec",
                    format!("tests-vex-client-{i}"),
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
                .run()?;
                // Apply 50ms latency to traffic leaving the server
                cmd!(
                    "docker",
                    "exec",
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
                .run()?;
            }
            "packet-loss" => {
                println!("Applying 5% packet loss...");
                cmd!(
                    "docker",
                    "exec",
                    format!("tests-vex-client-{i}"),
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
    println!("Building all Docker services...");
    cmd!("docker", "compose", "build",)
        .dir(root.join("xtask/tests"))
        .run()?;
    println!("Docker images built successfully.");
    Ok(())
}

fn run_correctness_task(
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
    let _env = DockerComposeEnv::new(project_root.join("xtask/tests").into_boxed_path(), clients)?;

    // --- Scenario-specific setup ---
    apply_scenario_conditions(scenario, clients)?;

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

    let results_file = test_results_dir.join("received_ids.txt");
    let received_ids: HashSet<u64> = fs::read_to_string(results_file)?
        .lines()
        .map(|line| line.parse().unwrap())
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

fn run_benchmark(root: Box<Path>, clients: u32) -> Result<(), XTaskError> {
    let test_results_dir = root.join("xtask/tests/test-results");
    // Clear up/delete existing contents in the test-results directory
    if test_results_dir.exists() {
        println!("Cleaning up existing test results...");
        fs::remove_dir_all(&test_results_dir)?;
    }
    fs::create_dir_all(&test_results_dir)?;

    // Wrap docker-compose commands in a struct with a Drop implementation for cleanup
    let _env = DockerComposeEnv::new(root.join("xtask/tests").into(), clients)?;

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

    // Read and analyze results
    let results_file = test_results_dir.join("received_ids.txt");

    // Better error handling for results file
    let messages_received = if results_file.exists() {
        let content = fs::read_to_string(&results_file)?;
        let unique_messages: std::collections::HashSet<_> = content.lines().collect();

        // Check for duplicates
        let total_lines = content.lines().count();
        if total_lines != unique_messages.len() {
            println!(
                "Warning: {} duplicate messages detected",
                total_lines - unique_messages.len()
            );
        }

        unique_messages.len()
    } else {
        eprintln!("Warning: Results file not found at {results_file:?}");
        0
    };

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
