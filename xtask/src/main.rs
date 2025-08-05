use clap::{Parser, Subcommand};
use duct::cmd;
use std::{collections::HashSet, env, fs, path::PathBuf};
use thiserror::Error;

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
}

#[derive(Error, Debug)]
enum TestingError {
    #[error("Test failed: {0}")]
    TestFailed(String),
    // #[error("Network condition error: {0}")]
    // NetworkConditionError(String),
}

fn main() -> Result<(), XTaskError> {
    let cli = Cli::parse();
    let project_root = env::current_dir()?;

    match cli.command {
        Commands::BuildDocker => build_docker(&project_root),
        Commands::TestE2e { scenario, clients } => {
            run_correctness_task(&project_root, &scenario, clients)
        }
        Commands::Benchmark { clients } => run_benchmark(&project_root, clients),
    }
}

// Helper struct for automatic cleanup
struct DockerComposeEnv {
    root: PathBuf,
}
impl DockerComposeEnv {
    fn new(root: &PathBuf, clients: u32) -> Result<Self, XTaskError> {
        fs::create_dir_all(root.join("test-results"))?;
        println!("Bringing up docker-compose environment...");
        cmd!(
            "docker",
            "compose",
            "up",
            "-d",
            "--scale",
            format!("vex-client={clients}"),
            "--no-recreate"
        )
        .dir(root)
        .run()?;
        println!("Waiting for environment to stabilize...");
        std::thread::sleep(std::time::Duration::from_secs(5));
        Ok(Self { root: root.clone() })
    }
}
impl Drop for DockerComposeEnv {
    fn drop(&mut self) {
        println!("Cleaning up docker-compose environment...");
        if let Err(e) = cmd!("docker", "compose", "down", "-v")
            .dir(&self.root)
            .run()
        {
            eprintln!("Failed to tear down docker environment: {}", e);
        }
    }
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
                // let index = format!("--index {}", i);
                // Apply 50ms latency to traffic leaving the client
                cmd!(
                    "docker",
                    "exec",
                    "--index",
                    (i).to_string(),
                    "vex-client",
                    "tc",
                    "qdisc",
                    "add",
                    "dev",
                    "eth0",
                    "root",
                    "netem",
                    "delay",
                    "50ms"
                )
                .run()?;
                // Apply 50ms latency to traffic leaving the server
                cmd!(
                    "docker",
                    "exec",
                    "vex-server",
                    "tc",
                    "qdisc",
                    "add",
                    "dev",
                    "eth0",
                    "root",
                    "netem",
                    "delay",
                    "50ms"
                )
                .run()?;
            }
            "packet-loss" => {
                println!("Applying 5% packet loss...");
                cmd!(
                    "docker",
                    "exec",
                    "--index",
                    (i).to_string(),
                    "vex-client",
                    "tc",
                    "qdisc",
                    "add",
                    "dev",
                    "eth0",
                    "root",
                    "netem",
                    "loss",
                    "5%"
                )
                .run()?;
            }
            _ => Err(XTaskError::Unexpected(format!(
                "Unknown scenario: {}",
                scenario
            )))?,
        }
    }
    Ok(())
}

fn build_docker(root: &PathBuf) -> Result<(), XTaskError> {
    println!("Building all Docker services...");
    cmd!("docker", "compose", "build",)
        .dir(&root.join("xtask/tests"))
        .run()?;
    println!("✅ Docker images built successfully.");
    Ok(())
}

fn run_correctness_task(
    project_root: &PathBuf,
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
    let _env = DockerComposeEnv::new(&project_root.join("xtask/tests"), clients)?;

    // --- Scenario-specific setup ---
    apply_scenario_conditions(&scenario, clients)?;

    // --- Execute Test ---
    let msg_count_per_client = 500;
    let total_msg_count = msg_count_per_client * clients as usize;

    println!(
        "Executing {} clients to send {} messages each...",
        clients, msg_count_per_client
    );
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
                // Pass CLIENT_ID to the container's environment
                // "--env",
                // &format!("CLIENT_ID={}", i - 1),
                "vex-client",
                "/usr/local/bin/test_client",
                "correctness",
                "--count",
                msg_count_per_client.to_string(),
                "--client-id",
                (i - 1).to_string()
            )
            .dir(&project_root.join("xtask/tests"))
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
                    "Missing message ID: {}",
                    i
                ))));
            }
        }
    }

    println!("✅ Scenario '{}' PASSED!", scenario);
    Ok(())
}

fn run_benchmark(root: &PathBuf, clients: u32) -> Result<(), XTaskError> {
    let test_results_dir = root.join("xtask/tests/test-results");
    // Clear up/delete existing contents in the test-results directory
    if test_results_dir.exists() {
        println!("Cleaning up existing test results...");
        fs::remove_dir_all(&test_results_dir)?;
    }
    fs::create_dir_all(&test_results_dir)?;

    println!("▶️  Running Benchmark: ");
    let _env = DockerComposeEnv::new(&root.join("xtask/tests"), clients)?;
    let duration_secs = 60; // Duration for the throughput test

    // Throughput requires a different client binary for now, one that just sends.
    // Or an updated client that takes a duration. For now, we'll simulate.
    println!("Executing throughput test for {} seconds...", duration_secs);
    let msg_count = 100;
    println!("Executing test client to send {} messages...", msg_count);
    cmd!(
        "docker",
        "compose",
        "exec",
        "-T", // No TTY allocation
        "vex-client",
        "/usr/local/bin/test_client",
        "latency",
        "--samples",
        msg_count.to_string()
    )
    .dir(&root.join("xtask/tests"))
    .run()?;
    println!("Client finished execution.");

    let results_file = test_results_dir.join("received_ids.txt");
    let count = fs::read_to_string(results_file)?.lines().count();
    let throughput = count as f64 / duration_secs as f64;
    println!("\n--- Throughput Benchmark Results ---");
    println!("Total messages received: {}", count);
    println!("Test duration: {} s", duration_secs);
    println!("Throughput: {:.2} msgs/sec", throughput);
    println!("------------------------------------\n");

    println!("✅ Benchmark finished.");
    Ok(())
}
