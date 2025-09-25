use networking::publisher::{AeronPublisher, PublisherError};
use networking::subscriber::{AeronSubscriber, SubscriberError};
use rusteron_client::{AeronFragmentAssembler, AeronFragmentHandlerCallback, AeronHeader, Handler};
use std::{
    cell::Cell,
    ffi::CString,
    sync::{Arc, Mutex},
    time::Duration,
};

// Fragment handler for testing
struct FragmentHandler {
    count: Cell<usize>,
    received_messages: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl FragmentHandler {
    fn new(received_messages: Arc<Mutex<Vec<Vec<u8>>>>) -> Self {
        Self {
            count: Cell::new(0),
            received_messages,
        }
    }
}

impl AeronFragmentHandlerCallback for FragmentHandler {
    fn handle_aeron_fragment_handler(&mut self, buffer: &[u8], _header: AeronHeader) {
        // Increment count
        self.count.set(self.count.get() + 1);

        // Store the received message
        if let Ok(mut messages) = self.received_messages.lock() {
            messages.push(buffer.to_vec());
        }
    }
}

// Helper function to create a test fragment assembler
fn create_test_fragment_assembler(
    received_messages: Arc<Mutex<Vec<Vec<u8>>>>,
) -> AeronFragmentAssembler {
    let handler = FragmentHandler::new(received_messages);
    AeronFragmentAssembler::new(Some(&Handler::leak(handler))).unwrap()
}

// Test helper to set up a publisher for a given channel and stream ID
fn setup_test_publisher(
    context_dir: &CString,
    channel: &str,
    stream_id: i32,
) -> Result<AeronPublisher, PublisherError> {
    let mut publisher = AeronPublisher::new(context_dir)?;
    publisher.add_publication(channel, stream_id)?;
    Ok(publisher)
}

// Test helper to set up a subscriber for a given channel and stream ID
fn setup_test_subscriber(
    context_dir: &CString,
    channel: &str,
    stream_id: i32,
) -> Result<(AeronSubscriber, Arc<Mutex<Vec<Vec<u8>>>>), SubscriberError> {
    let received_messages = Arc::new(Mutex::new(Vec::new()));
    let fragment_assembler = create_test_fragment_assembler(received_messages.clone());
    let mut subscriber = AeronSubscriber::new(context_dir, fragment_assembler)?;
    subscriber.add_subscription(channel, stream_id)?;
    subscriber.start();

    // Give subscriber time to start
    std::thread::sleep(Duration::from_millis(100));

    Ok((subscriber, received_messages))
}

#[test]
fn test_subscriber_creation() {
    // Test basic subscriber creation with valid context directory
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let received_messages = Arc::new(Mutex::new(Vec::new()));
    let fragment_assembler = create_test_fragment_assembler(received_messages);

    let subscriber = AeronSubscriber::new(&context_dir, fragment_assembler);
    assert!(subscriber.is_ok());
    subscriber.unwrap().stop().unwrap();
}

#[test]
fn test_add_subscription() {
    // Test adding a single subscription to the subscriber
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let received_messages = Arc::new(Mutex::new(Vec::new()));
    let fragment_assembler = create_test_fragment_assembler(received_messages);

    let mut subscriber = AeronSubscriber::new(&context_dir, fragment_assembler).unwrap();
    let result = subscriber.add_subscription("aeron:ipc", 3001);
    assert!(result.is_ok());

    subscriber.stop().unwrap();
}

#[test]
fn test_add_multiple_subscriptions() {
    // Test adding multiple subscriptions to the same subscriber
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let received_messages = Arc::new(Mutex::new(Vec::new()));
    let fragment_assembler = create_test_fragment_assembler(received_messages);

    let mut subscriber = AeronSubscriber::new(&context_dir, fragment_assembler).unwrap();

    assert!(subscriber.add_subscription("aeron:ipc", 3002).is_ok());
    assert!(
        subscriber
            .add_subscription("aeron:udp?endpoint=localhost:40123", 3003)
            .is_ok()
    );
    assert!(subscriber.add_subscription("aeron:ipc", 3004).is_ok());

    subscriber.stop().unwrap();
}

#[test]
fn test_receive_message() {
    // Test receiving a message from a publisher
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let (mut subscriber, received_messages) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 3005)
            .expect("Failed to set up test subscriber");

    let mut publisher = setup_test_publisher(&context_dir, "aeron:ipc", 3005)
        .expect("Failed to set up test publisher");

    let message = b"Hello, Subscriber!";
    let result = publisher.send(message, "aeron:ipc", 3005);
    assert!(result.is_ok());

    // Give time for message to be received
    std::thread::sleep(Duration::from_millis(500));

    if let Ok(messages) = received_messages.lock() {
        assert!(!messages.is_empty(), "No messages were received");
        assert_eq!(
            messages[0], b"Hello, Subscriber!",
            "Received message does not match expected"
        );
    } else {
        panic!("Failed to access received messages");
    }

    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_receive_multiple_messages() {
    // Test receiving multiple messages from a publisher
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let (mut subscriber, received_messages) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 3006)
            .expect("Failed to set up test subscriber");

    let mut publisher = setup_test_publisher(&context_dir, "aeron:ipc", 3006)
        .expect("Failed to set up test publisher");

    let messages = ["Message 1", "Message 2", "Message 3"];

    for message in &messages {
        let result = publisher.send(message.as_bytes(), "aeron:ipc", 3006);
        assert!(result.is_ok());
    }

    // Give time for messages to be received
    std::thread::sleep(Duration::from_millis(1000));

    if let Ok(received) = received_messages.lock() {
        assert_eq!(
            received.len(),
            3,
            "Expected 3 messages, got {}",
            received.len()
        );
        for (i, expected) in messages.iter().enumerate() {
            assert_eq!(
                &received[i],
                expected.as_bytes(),
                "Message {} does not match expected",
                i
            );
        }
    } else {
        panic!("Failed to access received messages");
    }

    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_receive_from_multiple_subscriptions() {
    // Test receiving messages from multiple subscriptions on the same subscriber
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let received_messages = Arc::new(Mutex::new(Vec::new()));
    let fragment_assembler = create_test_fragment_assembler(received_messages.clone());

    let mut subscriber = AeronSubscriber::new(&context_dir, fragment_assembler).unwrap();
    subscriber.add_subscription("aeron:ipc", 3007).unwrap();
    subscriber.add_subscription("aeron:ipc", 3008).unwrap();
    subscriber.start();

    // Give subscriber time to start
    std::thread::sleep(Duration::from_millis(100));

    let mut publisher1 = setup_test_publisher(&context_dir, "aeron:ipc", 3007)
        .expect("Failed to set up test publisher 1");
    let mut publisher2 = setup_test_publisher(&context_dir, "aeron:ipc", 3008)
        .expect("Failed to set up test publisher 2");

    let message1 = b"Message from stream 3007";
    let message2 = b"Message from stream 3008";

    publisher1.send(message1, "aeron:ipc", 3007).unwrap();
    publisher2.send(message2, "aeron:ipc", 3008).unwrap();

    // Give time for messages to be received
    std::thread::sleep(Duration::from_millis(500));

    if let Ok(messages) = received_messages.lock() {
        assert_eq!(
            messages.len(),
            2,
            "Expected 2 messages, got {}",
            messages.len()
        );
        assert!(
            messages.contains(&message1.to_vec()),
            "Message from stream 3007 not received"
        );
        assert!(
            messages.contains(&message2.to_vec()),
            "Message from stream 3008 not received"
        );
    } else {
        panic!("Failed to access received messages");
    }

    publisher1.stop().unwrap();
    publisher2.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_receive_large_message() {
    // Test receiving a large message (10KB)
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let (mut subscriber, received_messages) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 3009)
            .expect("Failed to set up test subscriber");

    let mut publisher = setup_test_publisher(&context_dir, "aeron:ipc", 3009)
        .expect("Failed to set up test publisher");

    let large_string_len = 10000;
    let binding = "2".repeat(large_string_len);
    let large_message = binding.as_bytes();

    let result = publisher.send(large_message, "aeron:ipc", 3009);
    assert!(result.is_ok());

    // Wait for messages to be received
    std::thread::sleep(Duration::from_millis(500));

    if let Ok(messages) = received_messages.lock() {
        assert!(!messages.is_empty(), "No messages were received");
        assert_eq!(
            messages[0], large_message,
            "Received message does not match expected"
        );
    }

    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_subscriber_stop() {
    // Test stopping the subscriber cleanly
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let received_messages = Arc::new(Mutex::new(Vec::new()));
    let fragment_assembler = create_test_fragment_assembler(received_messages);

    let mut subscriber = AeronSubscriber::new(&context_dir, fragment_assembler).unwrap();
    subscriber.add_subscription("aeron:ipc", 3010).unwrap();
    subscriber.start();

    // Give subscriber time to start
    std::thread::sleep(Duration::from_millis(100));

    let result = subscriber.stop();
    assert!(result.is_ok());
}

#[test]
fn test_invalid_channel_string() {
    // Test adding subscription with invalid channel string containing null byte
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let received_messages = Arc::new(Mutex::new(Vec::new()));
    let fragment_assembler = create_test_fragment_assembler(received_messages);

    let mut subscriber = AeronSubscriber::new(&context_dir, fragment_assembler).unwrap();

    let result = subscriber.add_subscription("aeron:ipc\0", 3011);
    assert!(matches!(result, Err(SubscriberError::InvalidInput(_))));

    subscriber.stop().unwrap();
}

#[test]
fn test_concurrent_publishers() {
    // Test receiving messages from multiple concurrent publishers
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let (mut subscriber, received_messages) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 3012)
            .expect("Failed to set up test subscriber");

    let mut publisher1 = setup_test_publisher(&context_dir, "aeron:ipc", 3012)
        .expect("Failed to set up test publisher 1");
    let mut publisher2 = setup_test_publisher(&context_dir, "aeron:ipc", 3012)
        .expect("Failed to set up test publisher 2");

    let message1 = b"Message from publisher 1";
    let message2 = b"Message from publisher 2";

    publisher1.send(message1, "aeron:ipc", 3012).unwrap();
    publisher2.send(message2, "aeron:ipc", 3012).unwrap();

    // Give time for messages to be received
    std::thread::sleep(Duration::from_millis(500));

    if let Ok(messages) = received_messages.lock() {
        assert_eq!(
            messages.len(),
            2,
            "Expected 2 messages, got {}",
            messages.len()
        );
        assert!(
            messages.contains(&message1.to_vec()),
            "Message from publisher 1 not received"
        );
        assert!(
            messages.contains(&message2.to_vec()),
            "Message from publisher 2 not received"
        );
    } else {
        panic!("Failed to access received messages");
    }

    publisher1.stop().unwrap();
    publisher2.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_mixed_channel_types() {
    // Test receiving messages from different channel types (IPC and UDP)
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let received_messages = Arc::new(Mutex::new(Vec::new()));
    let fragment_assembler = create_test_fragment_assembler(received_messages.clone());

    let mut subscriber = AeronSubscriber::new(&context_dir, fragment_assembler).unwrap();
    subscriber.add_subscription("aeron:ipc", 3013).unwrap();
    subscriber
        .add_subscription("aeron:udp?endpoint=localhost:40123", 3014)
        .unwrap();
    subscriber.start();

    // Give subscriber time to start
    std::thread::sleep(Duration::from_millis(100));

    let mut ipc_publisher = setup_test_publisher(&context_dir, "aeron:ipc", 3013)
        .expect("Failed to set up IPC publisher");
    let mut udp_publisher =
        setup_test_publisher(&context_dir, "aeron:udp?endpoint=localhost:40123", 3014)
            .expect("Failed to set up UDP publisher");

    let ipc_message = b"Message from IPC channel";
    let udp_message = b"Message from UDP channel";

    ipc_publisher.send(ipc_message, "aeron:ipc", 3013).unwrap();
    udp_publisher
        .send(udp_message, "aeron:udp?endpoint=localhost:40123", 3014)
        .unwrap();

    // Give time for messages to be received
    std::thread::sleep(Duration::from_millis(500));

    if let Ok(messages) = received_messages.lock() {
        assert_eq!(
            messages.len(),
            2,
            "Expected 2 messages, got {}",
            messages.len()
        );
        assert!(
            messages.contains(&ipc_message.to_vec()),
            "IPC message not received"
        );
        assert!(
            messages.contains(&udp_message.to_vec()),
            "UDP message not received"
        );
    } else {
        panic!("Failed to access received messages");
    }

    ipc_publisher.stop().unwrap();
    udp_publisher.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_message_ordering() {
    // Test that messages are received in the order they were sent
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let (mut subscriber, received_messages) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 3015)
            .expect("Failed to set up test subscriber");

    let mut publisher = setup_test_publisher(&context_dir, "aeron:ipc", 3015)
        .expect("Failed to set up test publisher");

    let messages = [
        "First message",
        "Second message",
        "Third message",
        "Fourth message",
        "Fifth message",
    ];

    for message in &messages {
        publisher
            .send(message.as_bytes(), "aeron:ipc", 3015)
            .unwrap();
        // Small delay to ensure ordering
        std::thread::sleep(Duration::from_millis(10));
    }

    // Give time for messages to be received
    std::thread::sleep(Duration::from_millis(1000));

    if let Ok(received) = received_messages.lock() {
        assert_eq!(
            received.len(),
            5,
            "Expected 5 messages, got {}",
            received.len()
        );
        for (i, expected) in messages.iter().enumerate() {
            assert_eq!(
                &received[i],
                expected.as_bytes(),
                "Message {} does not match expected order",
                i
            );
        }
    } else {
        panic!("Failed to access received messages");
    }

    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}
