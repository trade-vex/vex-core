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
fn test_publisher_creation() {
    // Test basic publisher creation with valid context directory
    let context_dir = CString::new("/home/esciiee/vex/aeron/aeron-samples/scripts")
        .expect("Failed to create CString");
    let (mut subscriber, _received_messages) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 2001)
            .expect("Failed to set up test subscriber");

    let publisher = AeronPublisher::new(&context_dir);
    assert!(publisher.is_ok());
    publisher.unwrap().stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_add_publication() {
    // Test adding a single publication to the publisher
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let (mut subscriber, _received_messages) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 2002)
            .expect("Failed to set up test subscriber");

    let mut publisher = AeronPublisher::new(&context_dir).unwrap();
    let result = publisher.add_publication("aeron:ipc", 2002);
    assert!(result.is_ok());

    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_add_multiple_publications() {
    // Test adding multiple publications with different channels and stream IDs
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");

    let (mut subscriber1, _received_messages1) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 2003)
            .expect("Failed to set up test subscriber 1");
    let (mut subscriber2, _received_messages2) =
        setup_test_subscriber(&context_dir, "aeron:udp?endpoint=localhost:40123", 2004)
            .expect("Failed to set up test subscriber 2");
    let (mut subscriber3, _received_messages3) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 2005)
            .expect("Failed to set up test subscriber 3");

    let mut publisher = AeronPublisher::new(&context_dir).unwrap();

    assert!(publisher.add_publication("aeron:ipc", 2003).is_ok());
    assert!(
        publisher
            .add_publication("aeron:udp?endpoint=localhost:40123", 2004)
            .is_ok()
    );
    assert!(publisher.add_publication("aeron:ipc", 2005).is_ok());

    publisher.stop().unwrap();
    subscriber1.stop().unwrap();
    subscriber2.stop().unwrap();
    subscriber3.stop().unwrap();
}

#[test]
fn test_send_message() {
    // Test sending a message to a specific publication and verifying reception
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let (mut subscriber, received_messages) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 2006)
            .expect("Failed to set up test subscriber");

    let mut publisher = AeronPublisher::new(&context_dir).unwrap();
    publisher.add_publication("aeron:ipc", 2006).unwrap();

    let message = b"Hello, Aeron!";
    let result = publisher.send(message, "aeron:ipc", 2006);
    assert!(result.is_ok());

    // Give time for message to be received
    std::thread::sleep(Duration::from_millis(500));

    if let Ok(messages) = received_messages.lock() {
        assert!(!messages.is_empty(), "No messages were received");
        assert_eq!(
            messages[0], b"Hello, Aeron!",
            "Received message does not match expected"
        );
    } else {
        panic!("Failed to access received messages");
    }

    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_send_message_to_nonexistent_publication() {
    // Test sending to a publication that hasn't been added
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let (mut subscriber, _received_messages) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 2007)
            .expect("Failed to set up test subscriber");

    let mut publisher = AeronPublisher::new(&context_dir).unwrap();

    let message = b"Hello, Aeron!";
    let result = publisher.send(message, "aeron:ipc", 2007);
    assert!(result.is_err());

    if let Err(error) = result {
        assert_eq!(error, PublisherError::PublicationNotFound);
    } else {
        panic!("Expected error PublisherError::PublicationNotFound, but got Ok");
    }

    if let Ok(messages) = _received_messages.lock() {
        assert!(
            messages.is_empty(),
            "Messages were still received when no publication exists"
        );
    } else {
        panic!("Failed to access received messages");
    }

    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_send_empty_message() {
    // Test sending an empty message which should be rejected
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let (mut subscriber, received_messages) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 2008)
            .expect("Failed to set up test subscriber");

    let mut publisher = AeronPublisher::new(&context_dir).unwrap();
    publisher.add_publication("aeron:ipc", 2008).unwrap();

    let result = publisher.send(&[], "aeron:ipc", 2008);
    assert!(result.is_err());

    if let Err(error) = result {
        assert_eq!(error, PublisherError::EmptyMessage);
    } else {
        panic!("Expected error PublisherError::EmptyMessage, but got Ok");
    }

    if let Ok(messages) = received_messages.lock() {
        assert!(messages.is_empty());
    } else {
        panic!("Failed to access received messages");
    }

    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_send_all_messages() {
    // Test broadcasting a message to all publications
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");

    let (mut subscriber1, received_messages1) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 2009)
            .expect("Failed to set up test subscriber 1");
    let (mut subscriber2, received_messages2) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 2010)
            .expect("Failed to set up test subscriber 2");

    let mut publisher = AeronPublisher::new(&context_dir).unwrap();
    publisher.add_publication("aeron:ipc", 2009).unwrap();
    publisher.add_publication("aeron:ipc", 2010).unwrap();

    let message = b"Broadcast message";
    let result = publisher.send_all(message);
    assert!(result.is_ok());

    // Wait for messages to be received
    std::thread::sleep(Duration::from_millis(100));

    if let Ok(messages) = received_messages1.lock() {
        assert!(!messages.is_empty(), "No messages were received");
        assert_eq!(
            messages[0], b"Broadcast message",
            "Received message does not match expected"
        );
    } else {
        panic!("Failed to access received messages");
    }

    if let Ok(messages) = received_messages2.lock() {
        assert!(!messages.is_empty(), "No messages were received");
        assert_eq!(
            messages[0], b"Broadcast message",
            "Received message does not match expected"
        );
    } else {
        panic!("Failed to access received messages");
    }

    publisher.stop().unwrap();
    subscriber1.stop().unwrap();
    subscriber2.stop().unwrap();
}

#[test]
fn test_send_all_empty_message() {
    // Test broadcasting an empty message which should be rejected
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let (mut subscriber, received_messages) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 2011)
            .expect("Failed to set up test subscriber");

    let mut publisher = AeronPublisher::new(&context_dir).unwrap();
    publisher.add_publication("aeron:ipc", 2011).unwrap();

    let result = publisher.send_all(&[]);
    assert!(result.is_err());

    if let Err(error) = result {
        assert_eq!(error, PublisherError::EmptyMessage);
    } else {
        panic!("Expected error PublisherError::EmptyMessage, but got Ok");
    }

    if let Ok(messages) = received_messages.lock() {
        assert!(messages.is_empty());
    } else {
        panic!("Failed to access received messages");
    }

    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_stop_publisher() {
    // Test stopping the publisher cleanly
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let (mut subscriber, _received_messages) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 2012)
            .expect("Failed to set up test subscriber");

    let mut publisher = AeronPublisher::new(&context_dir).unwrap();
    publisher.add_publication("aeron:ipc", 2012).unwrap();
    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_send_after_stop() {
    // Test that sending after stopping the publisher returns an error
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let (mut subscriber, _received_messages) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 2013)
            .expect("Failed to set up test subscriber");

    let mut publisher = AeronPublisher::new(&context_dir).unwrap();
    publisher.add_publication("aeron:ipc", 2013).unwrap();
    publisher.stop().unwrap();

    let message = b"Should not be sent";
    let result = publisher.send(message, "aeron:ipc", 2013);
    assert!(result.is_err());

    if let Err(error) = result {
        assert_eq!(error, PublisherError::NotRunning);
    } else {
        panic!("Expected error PublisherError::NotRunning, but got Ok");
    }

    subscriber.stop().unwrap();
}

#[test]
fn test_large_message_send() {
    // Test sending a large message (10KB) successfully
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let (mut subscriber, received_messages) =
        setup_test_subscriber(&context_dir, "aeron:ipc", 2014)
            .expect("Failed to set up test subscriber");

    let mut publisher = AeronPublisher::new(&context_dir).unwrap();
    publisher.add_publication("aeron:ipc", 2014).unwrap();

    let large_string_len = 10000;
    let binding = "1".repeat(large_string_len);
    let large_message = binding.as_bytes();

    let result = publisher.send(large_message, "aeron:ipc", 2014);
    assert!(result.is_ok());

    // Wait for messages to be received
    std::thread::sleep(Duration::from_millis(100));

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
fn test_invalid_channel_string() {
    // Test adding publication with invalid channel string containing null byte
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let mut publisher = AeronPublisher::new(&context_dir).unwrap();

    let result = publisher.add_publication("aeron:ipc\0", 2015);
    assert!(matches!(result, Err(PublisherError::InvalidInput(_))));

    publisher.stop().unwrap();
}
