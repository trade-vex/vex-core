use server::init_exchange;
use vex_config::{Environment, VexConfig};

fn main() {
    let config = VexConfig::new(Environment::Test);

    let (mut engine, producer, _events_handler) = init_exchange();

    engine.run(producer, config.core_networking);
}
