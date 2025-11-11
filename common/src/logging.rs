/// Helper macros for producing consistent structured logs around `OrderCommand` processing.
///
/// These macros ensure we always emit the same contextual fields when logging anything related
/// to the order pipeline. Additional fields can be appended as key-value pairs.
#[macro_export]
macro_rules! order_log {
    ($level:ident, $event:expr, $cmd:expr $(, $($field:tt)*)?) => {{
        let cmd_ref = &$cmd;
        tracing::$level!(
            target: "order_pipeline",
            event = $event,
            order_id = cmd_ref.order_id,
            client_order_id = cmd_ref.client_order_id,
            user_id = cmd_ref.user_id,
            market_id = cmd_ref.market_id,
            command = ?cmd_ref.command,
            status = ?cmd_ref.status,
            side = ?cmd_ref.side,
            price = cmd_ref.price,
            size = cmd_ref.size,
            $($($field)*)?
        );
    }};
}

/// Structured `info!` log for an order command.
#[macro_export]
macro_rules! order_info {
    ($event:expr, $cmd:expr $(, $($field:tt)*)?) => {
        $crate::order_log!(info, $event, $cmd $(, $($field)*)?);
    };
}

/// Structured `debug!` log for an order command.
#[macro_export]
macro_rules! order_debug {
    ($event:expr, $cmd:expr $(, $($field:tt)*)?) => {
        $crate::order_log!(debug, $event, $cmd $(, $($field)*)?);
    };
}

/// Structured `warn!` log for an order command.
#[macro_export]
macro_rules! order_warn {
    ($event:expr, $cmd:expr $(, $($field:tt)*)?) => {
        $crate::order_log!(warn, $event, $cmd $(, $($field)*)?);
    };
}

/// Structured `error!` log for an order command.
#[macro_export]
macro_rules! order_error {
    ($event:expr, $cmd:expr $(, $($field:tt)*)?) => {
        $crate::order_log!(error, $event, $cmd $(, $($field)*)?);
    };
}
