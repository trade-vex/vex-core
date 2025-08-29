use clap::{Parser, Subcommand};
use common::cmd::{OrderCommand, OrderCommandType, decode_order_command};
use common::model::enums::{OrderType, Side};
use hdrhistogram::Histogram;
use rusteron_client::{AeronFragmentHandlerCallback, AeronHeader};
use std::env;
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::{Duration, Instant};
use tracing::{error, info};
use vex_config::GatewayNetworkingConfig;
use vex_networking::client::VexGateway;

#[derive(Parser, Debug)]
#[command(name = "test_client")]
struct Cli {
    #[command(subcommand)]
    mode: Mode,

    #[arg(long, default_value_t = 0)]
    client_id: u64,
}

#[derive(Subcommand, Debug)]
enum Mode {
    /// Send a fixed number of messages for correctness testing.
    Correctness {
        #[arg(short, long, default_value_t = 100)]
        count: u64,
    },
    /// Measure round-trip latency for a number of samples.
    Latency {
        #[arg(short, long, default_value_t = 1000)]
        samples: u64,
    },
}

struct OrderCommandHandler {
    gateway_id: String,
    sender: Sender<OrderCommand>,
}

impl AeronFragmentHandlerCallback for OrderCommandHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        // Deserialize OrderCommand
        match decode_order_command(buffer) {
            Ok(order_command) => {
                info!(
                    "Gateway {}: Received OrderCommand: {:?}",
                    self.gateway_id, order_command
                );

                self.sender.send(order_command).unwrap_or_else(|e| {
                    error!(
                        "Gateway {}: Failed to send OrderCommand to channel: {:?}",
                        self.gateway_id, e
                    );
                });
            }
            Err(e) => {
                error!(
                    "Gateway {}: Failed to decode OrderCommand: {:?}",
                    self.gateway_id, e
                );
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    // Read configuration from environment variables provided by docker-compose
    // start logging
    tracing_subscriber::fmt::init();
    let server_host = env::var("VEX_SERVER_HOST").unwrap_or("127.0.0.1".to_string());
    let server_port: u16 = env::var("VEX_SERVER_PORT")?.parse()?;
    // let sleep_duration = if args.rate > 0 { Duration::from_micros(1_000_000 / args.rate) } else { Duration::ZERO };

    println!(
        "Client starting. Attempting to connect to {}:{}",
        server_host, server_port
    );

    let mut client_config = GatewayNetworkingConfig::test_defaults(); // Use your test defaults
    client_config.context_dir =
        env::var("VEX_CONTEXT_DIR").unwrap_or("/dev/shm/aeron-test-client".to_string());
    client_config.core_address = server_host;
    client_config.core_port = server_port;
    client_config.core_control_port = server_port + 1;
    client_config.gateway_id = format!("test-gateway-{}", args.client_id);

    let mut client = VexGateway::new(client_config)?;
    let (sx, mut rx) = mpsc::channel();
    let handler = OrderCommandHandler {
        gateway_id: client.gateway_id().to_string(),
        sender: sx,
    };
    match client.start(handler) {
        Ok(()) => println!("Client run() completed successfully"),
        Err(e) => println!("Client run() error: {e}"),
    }

    // The client's main loop to send commands
    match args.mode {
        Mode::Correctness { count } => {
            run_correctness_test(&mut client, count, args.client_id)?;
        }
        Mode::Latency { samples } => {
            // For this to work, the client needs a way to receive acks.
            // This is a conceptual implementation.
            println!("NOTE: Latency test requires the client to be able to receive messages.");
            run_latency_test(&mut client, &mut rx, samples)?;
        }
    }
    Ok(())
}

fn run_correctness_test(
    client: &mut VexGateway,
    count: u64,
    client_id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Client-{} sending {} messages...", client_id, count);
    for i in 0..count {
        let order_command = OrderCommand {
            command: OrderCommandType::PlaceLimitOrder,
            user_id: 1,
            reserve_bid_price: 150,
            size: 100,
            order_type: OrderType::Gtc,
            timestamp: 1,
            matcher_event: None,
            side: Side::Ask,
            order_id: client_id * 1_000_000 + i,
            symbol_id: 3124,
            price: 150,
        };
        client.send_order_command(&order_command)?;
    }
    println!("Client finished sending.");
    std::thread::sleep(Duration::from_secs(2));
    Ok(())
}

fn run_latency_test(
    client: &mut VexGateway,
    rx: &mut Receiver<OrderCommand>,
    samples: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut histogram = Histogram::<u64>::new(3).unwrap();

    for i in 0..samples {
        let mut command = OrderCommand {
            command: OrderCommandType::PlaceLimitOrder,
            user_id: 1,
            reserve_bid_price: 150,
            size: 100,
            order_type: OrderType::Gtc,
            timestamp: 1,
            matcher_event: None,
            side: Side::Ask,
            order_id: i,
            symbol_id: 3124,
            price: 150,
        };

        let start_time = Instant::now();
        // The timestamp field is used to carry the start time as nanoseconds
        command.timestamp = start_time.elapsed().as_nanos() as u64; // This is a placeholder for a real timestamping mechanism

        client.send_order_command(&command)?;

        // --- Conceptual: Wait for the acknowledgment ---
        let ack = rx.recv_timeout(Duration::from_secs(5))?;
        if ack.order_id == i {
            let rtt = start_time.elapsed().as_micros() as u64;
            histogram.record(rtt).unwrap();
        }
    }

    println!("\n--- Latency Benchmark Results ---");
    println!("Total Samples: {}", histogram.len());
    println!("p50 (Median):  {} µs", histogram.value_at_percentile(50.0));
    println!("p90:           {} µs", histogram.value_at_percentile(90.0));
    println!("p99:           {} µs", histogram.value_at_percentile(99.0));
    println!("p99.9:         {} µs", histogram.value_at_percentile(99.9));
    println!("Max:           {} µs", histogram.max());
    println!("---------------------------------\n");

    Ok(())
}
