use clap::{Parser, Subcommand};
use common::{OrderCommand, UserBalance};
use common::{OrderCommandType, Side, Status, TimeInForce};
use std::fs::File;
use std::io::Write;
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};
use vex_config::GatewayNetworkingConfig;
use vex_networking::client::{OrderCommandHandler, Publisher, VexGateway};

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
    /// Fund two traders, then send `count` orders that cross one another, and record the id of
    /// every answer.
    Correctness {
        #[arg(short, long, default_value_t = 100)]
        count: u64,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
    // Read configuration from environment variables provided by docker-compose
    // start logging
    tracing_subscriber::fmt::init();
    let server_host = std::env::var("VEX_SERVER_HOST").unwrap_or("127.0.0.1".to_string());
    let server_port: u16 = std::env::var("VEX_SERVER_PORT")
        .unwrap_or("3521".to_string())
        .parse()?;

    println!("Client starting. Attempting to connect to {server_host}:{server_port}");

    let mut client_config = GatewayNetworkingConfig::test_defaults(); // Use your test defaults
    client_config.core_address = server_host;
    client_config.core_port = server_port;
    client_config.core_control_port = server_port + 1;
    client_config.gateway_id = args.client_id as u8;

    let mut client = VexGateway::new(client_config)?;
    let (sx, mut rx) = mpsc::channel();
    let handler = OrderCommandHandler::new(client.gateway_id(), sx);
    // A REJECT or UNAVAILABLE ends the run with the core's reason.
    let publisher = client.start(handler)?;

    // The client's main loop to send commands
    match args.mode {
        Mode::Correctness { count } => {
            run_correctness_test(&publisher, &mut rx, count, args.client_id)?;
        }
    }
    Ok(())
}

fn run_correctness_test(
    client: &Publisher,
    rx: &mut Receiver<OrderCommand>,
    count: u64,
    client_id: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Client-{client_id} sending {count} messages...");
    let base_asset_id = 2;
    let quote_asset_id = 1;
    // Market ID: base asset in lower 16 bits, quote in upper 16
    let market_id = ((quote_asset_id as u32) << 16) | (base_asset_id as u32);

    // Twice what the orders lock: count/2 bids of 100 at 150 (unscaled notional, above the cost
    // on any market) and count/2 asks of 100. The acks are taken here: a deposit's
    // client_order_id is 0, as the first order's is.
    let started = Instant::now();
    for deposit in [
        OrderCommand::deposit_funds(1, count * 150 * 100, quote_asset_id),
        OrderCommand::deposit_funds(2, count * 100, base_asset_id),
    ] {
        loop {
            client.send_order_command(&deposit)?;
            match rx.recv_timeout(Duration::from_secs(5))?.status {
                Status::Processed => break,
                // Another client's orders filled the ring, nothing executed; 30 s of it is a wedge.
                Status::Busy if started.elapsed() < Duration::from_secs(30) => continue,
                status => {
                    return Err(format!(
                        "Client-{client_id}: a deposit was answered {status:?} after {:?}",
                        started.elapsed()
                    )
                    .into());
                }
            }
        }
    }

    // Every response, duplicates included: xtask fails the run on an id answered twice.
    let mut received_ids = Vec::new();
    let mut filled = 0;
    let order = |i: u64| OrderCommand {
        client_order_id: i,
        command: OrderCommandType::PlaceOrder,
        user_id: if i.is_multiple_of(2) { 1 } else { 2 },
        size: 100,
        time_in_force: TimeInForce::Gtc,
        timestamp: 1,
        side: if i.is_multiple_of(2) {
            Side::Bid
        } else {
            Side::Ask
        },
        order_id: client_id * 1_000_000 + i,
        market_id,
        price: 150,
        status: common::Status::Processing,
        events: None,
        balance: [UserBalance::default(); 2],
        l2_data: None,
        route_gateway_id: 0,
        original_size: 0,
    };

    // At most WINDOW orders unanswered, as a gateway paces its users: the most gateways the core
    // takes (16) stay well under its 1,024-slot ring, so no client's orders are all answered Busy.
    const WINDOW: u64 = 32;
    let mut sent = 0;
    let mut last_answer = Instant::now();
    while received_ids.len() < count as usize && last_answer.elapsed() < Duration::from_secs(30) {
        if sent < count && sent.saturating_sub(received_ids.len() as u64) < WINDOW {
            client.send_order_command(&order(sent))?;
            sent += 1;
            continue;
        }
        if let Ok(response) = rx.recv_timeout(Duration::from_millis(100)) {
            last_answer = Instant::now();
            if matches!(response.status, Status::Filled | Status::PartiallyFilled) {
                filled += 1;
            }
            received_ids.push(response.client_order_id);
            if received_ids.len() % 100 == 0 {
                println!(
                    "Client-{client_id} received {}/{count} responses",
                    received_ids.len()
                );
            }
        }
    }

    // A duplicate that arrives after the last expected response must still be seen.
    while let Ok(response) = rx.recv_timeout(Duration::from_millis(500)) {
        received_ids.push(response.client_order_id);
    }

    println!(
        "Client-{client_id} received {}/{count} responses",
        received_ids.len()
    );

    write_received_ids(client_id, received_ids)?;

    // Orders that no longer match (all rejected, say) would pass the id check without a fill.
    if filled == 0 {
        return Err(format!("Client-{client_id}: no order was filled").into());
    }
    println!("Client-{client_id} finished, {filled} orders filled.");
    Ok(())
}

/// Write the ids this client got a response for, offset by `client_id * 1_000_000`, to
/// `/results/received_ids-<client_id>.txt`, where xtask reads them back. Every client has its own
/// file because all of them write to the same `/results` mount.
fn write_received_ids(
    client_id: u64,
    ids: impl IntoIterator<Item = u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::create(format!("/results/received_ids-{client_id}.txt"))?;
    for id in ids {
        writeln!(file, "{}", client_id * 1_000_000 + id)?;
    }
    Ok(())
}
