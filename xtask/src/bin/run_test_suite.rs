//! VEX-CORE Integration Test Suite CLI Runner
//!
//! This binary executes the comprehensive integration test suite
//! for the VEX-CORE trading system.

use clap::{Parser, Subcommand};
use std::process;
use tracing::{error, info};
use tracing_subscriber::FmtSubscriber;
use xtask::scenarios;
use xtask::test_framework::TestContext;
use xtask::test_framework::types::TestSuiteResult;

#[derive(Parser)]
#[command(name = "run_test_suite")]
#[command(about = "VEX-CORE Integration Test Suite", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// Verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Fail fast on first error
    #[arg(short, long)]
    fail_fast: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Run comprehensive integration test (all order types + edge cases in single state)
    All,

    /// Run balance management tests
    Balance,

    /// Run GTC order tests
    Gtc,

    /// Run IOC order tests
    Ioc,

    /// Run FOK order tests
    Fok,

    /// Run cancellation tests
    Cancellation,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Initialize logging
    let subscriber = if cli.verbose {
        FmtSubscriber::builder()
            .with_max_level(tracing::Level::DEBUG)
            .finish()
    } else {
        FmtSubscriber::builder()
            .with_max_level(tracing::Level::INFO)
            .finish()
    };
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");

    info!("Starting VEX-CORE Integration Test Suite");
    info!("========================================");

    // Create test context
    let mut ctx = match TestContext::new().await {
        Ok(ctx) => ctx,
        Err(e) => {
            error!("Failed to initialize test context: {}", e);
            process::exit(1);
        }
    };

    let command = cli.command.unwrap_or(Commands::All);

    let mut suite_result = TestSuiteResult::new();
    let start = std::time::Instant::now();

    match command {
        Commands::All => {
            info!("Running Comprehensive Integration Test Suite");
            info!("(All order types + edge cases in single continuous state)");
            info!("");

            // Run comprehensive integration test
            // This is a single massive test combining GTC, IOC, FOK, and cancellations
            // with edge cases, stress testing, and realistic trading scenarios
            match scenarios::comprehensive::run_all(&mut ctx).await {
                Ok(results) => {
                    for result in results {
                        suite_result.add_result(result);
                    }
                }
                Err(e) => {
                    error!("Comprehensive integration test failed: {}", e);
                    if cli.fail_fast {
                        process::exit(1);
                    }
                }
            }
        }
        Commands::Balance => {
            info!("Running Balance Management tests");
            match scenarios::balance::run_all(&mut ctx).await {
                Ok(results) => {
                    for result in results {
                        suite_result.add_result(result);
                    }
                }
                Err(e) => {
                    error!("Balance tests failed: {}", e);
                    process::exit(1);
                }
            }
        }
        Commands::Gtc => {
            info!("Running GTC Order tests");
            match scenarios::gtc::run_all(&mut ctx).await {
                Ok(results) => {
                    for result in results {
                        suite_result.add_result(result);
                    }
                }
                Err(e) => {
                    error!("GTC tests failed: {}", e);
                    process::exit(1);
                }
            }
        }
        Commands::Ioc => {
            info!("Running IOC Order tests");
            match scenarios::ioc::run_all(&mut ctx).await {
                Ok(results) => {
                    for result in results {
                        suite_result.add_result(result);
                    }
                }
                Err(e) => {
                    error!("IOC tests failed: {}", e);
                    process::exit(1);
                }
            }
        }
        Commands::Fok => {
            info!("Running FOK Order tests");
            match scenarios::fok::run_all(&mut ctx).await {
                Ok(results) => {
                    for result in results {
                        suite_result.add_result(result);
                    }
                }
                Err(e) => {
                    error!("FOK tests failed: {}", e);
                    process::exit(1);
                }
            }
        }
        Commands::Cancellation => {
            info!("Running Cancellation tests");
            match scenarios::cancellation::run_all(&mut ctx).await {
                Ok(results) => {
                    for result in results {
                        suite_result.add_result(result);
                    }
                }
                Err(e) => {
                    error!("Cancellation tests failed: {}", e);
                    process::exit(1);
                }
            }
        }
    }

    suite_result.duration = start.elapsed();

    // Cleanup
    if let Err(e) = ctx.cleanup().await {
        error!("Cleanup failed: {}", e);
    }

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
            if let Some(ref error) = result.error {
                error!("    Error: {}", error);
            }
        }
    }

    info!("========================================");

    // Exit with appropriate code
    if suite_result.is_success() {
        info!("All tests passed! ✓");
        process::exit(0);
    } else {
        error!("Some tests failed! ✗");
        process::exit(1);
    }
}
