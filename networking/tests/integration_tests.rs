use networking::subscriber::{AeronSubscriber, SubscriberError};
use networking::publisher::{AeronPublisher, PublisherError};
use rusteron_client::{
    AeronFragmentAssembler, AeronFragmentHandlerCallback, AeronHeader, Handler
};
use std::{
    cell::Cell,
    ffi::CString,
    sync::{Arc, Mutex},
    time::Duration,
    thread,
    net::{UdpSocket},
};

// Fragment handler for testing
struct FragmentHandler {
    count: Cell<usize>,
    received_messages: Arc<Mutex<Vec<Vec<u8>>>>,
    message_sizes: Arc<Mutex<Vec<usize>>>,
    timestamps: Arc<Mutex<Vec<std::time::Instant>>>,
}

impl FragmentHandler {
    fn new(received_messages: Arc<Mutex<Vec<Vec<u8>>>>) -> Self {
        Self {
            count: Cell::new(0),
            received_messages,
            message_sizes: Arc::new(Mutex::new(Vec::new())),
            timestamps: Arc::new(Mutex::new(Vec::new())),
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
        
        // Store message size
        if let Ok(mut sizes) = self.message_sizes.lock() {
            sizes.push(buffer.len());
        }
        
        // Store timestamp
        if let Ok(mut timestamps) = self.timestamps.lock() {
            timestamps.push(std::time::Instant::now());
        }
    }
}

// Helper function to create a test fragment assembler
fn create_test_fragment_assembler(received_messages: Arc<Mutex<Vec<Vec<u8>>>>) -> AeronFragmentAssembler {
    let handler = FragmentHandler::new(received_messages);
    AeronFragmentAssembler::new(Some(&Handler::leak(handler))).unwrap()
}

// Test helper to set up a publisher for a given channel and stream ID
fn setup_test_publisher(context_dir: &CString, channel: &str, stream_id: i32) -> Result<AeronPublisher, PublisherError> {
    let mut publisher = AeronPublisher::new(context_dir)?;
    publisher.add_publication(channel, stream_id)?;
    Ok(publisher)
}

// Test helper to set up a subscriber for a given channel and stream ID
fn setup_test_subscriber(context_dir: &CString, channel: &str, stream_id: i32) -> Result<(AeronSubscriber, Arc<Mutex<Vec<Vec<u8>>>>), SubscriberError> {
    let received_messages = Arc::new(Mutex::new(Vec::new()));
    let fragment_assembler = create_test_fragment_assembler(received_messages.clone());
    let mut subscriber = AeronSubscriber::new(context_dir, fragment_assembler)?;
    subscriber.add_subscription(channel, stream_id)?;
    subscriber.start();
    
    // Give subscriber time to start
    std::thread::sleep(Duration::from_millis(100));
    
    Ok((subscriber, received_messages))
}

// Helper to find available UDP ports
fn find_available_udp_port() -> u16 {
    for port in 40123..40200 {
        if UdpSocket::bind(format!("127.0.0.1:{}", port)).is_ok() {
            return port;
        }
    }
    40123 // fallback
}

#[test]
fn test_basic_udp_communication() {
    // Test basic UDP communication between publisher and subscriber
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let port = find_available_udp_port();
    let channel = format!("aeron:udp?endpoint=localhost:{}", port);
    
    let (mut subscriber, received_messages) = setup_test_subscriber(&context_dir, &channel, 4001)
        .expect("Failed to set up test subscriber");
    
    let mut publisher = setup_test_publisher(&context_dir, &channel, 4001)
        .expect("Failed to set up test publisher");
    
    let message = b"Hello, UDP World!";
    let result = publisher.send(message, &channel, 4001);
    assert!(result.is_ok());
    
    // Give time for message to be received
    std::thread::sleep(Duration::from_millis(500));
    
    if let Ok(messages) = received_messages.lock() {
        assert!(!messages.is_empty(), "No messages were received");
        assert_eq!(messages[0], message, "Received message does not match expected");
    } else {
        panic!("Failed to access received messages");
    }
    
    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_multiple_udp_endpoints() {
    // Test communication across multiple UDP endpoints
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let port1 = find_available_udp_port();
    let port2 = find_available_udp_port();
    
    let channel1 = format!("aeron:udp?endpoint=localhost:{}", port1);
    let channel2 = format!("aeron:udp?endpoint=localhost:{}", port2);
    
    let received_messages = Arc::new(Mutex::new(Vec::new()));
    let fragment_assembler = create_test_fragment_assembler(received_messages.clone());
    
    let mut subscriber = AeronSubscriber::new(&context_dir, fragment_assembler).unwrap();
    subscriber.add_subscription(&channel1, 4002).unwrap();
    subscriber.add_subscription(&channel2, 4003).unwrap();
    subscriber.start();
    
    std::thread::sleep(Duration::from_millis(100));
    
    let mut publisher1 = setup_test_publisher(&context_dir, &channel1, 4002).unwrap();
    let mut publisher2 = setup_test_publisher(&context_dir, &channel2, 4003).unwrap();
    
    let message1 = b"Message from endpoint 1";
    let message2 = b"Message from endpoint 2";
    
    publisher1.send(message1, &channel1, 4002).unwrap();
    publisher2.send(message2, &channel2, 4003).unwrap();
    
    std::thread::sleep(Duration::from_millis(500));
    
    if let Ok(messages) = received_messages.lock() {
        assert_eq!(messages.len(), 2, "Expected 2 messages, got {}", messages.len());
        assert!(messages.contains(&message1.to_vec()), "Message from endpoint 1 not received");
        assert!(messages.contains(&message2.to_vec()), "Message from endpoint 2 not received");
    } else {
        panic!("Failed to access received messages");
    }
    
    publisher1.stop().unwrap();
    publisher2.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_udp_broadcast_simulation() {
    // Test simulating broadcast behavior with multiple subscribers
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let port = find_available_udp_port();
    let channel = format!("aeron:udp?endpoint=localhost:{}", port);
    
    // Create multiple subscribers
    let (mut subscriber1, received_messages1) = setup_test_subscriber(&context_dir, &channel, 4004).unwrap();
    let (mut subscriber2, received_messages2) = setup_test_subscriber(&context_dir, &channel, 4004).unwrap();
    let (mut subscriber3, received_messages3) = setup_test_subscriber(&context_dir, &channel, 4004).unwrap();
    
    let mut publisher = setup_test_publisher(&context_dir, &channel, 4004).unwrap();
    
    let message = b"Broadcast message to all subscribers";
    publisher.send(message, &channel, 4004).unwrap();
    
    std::thread::sleep(Duration::from_millis(500));
    
    // Verify all subscribers received the message
    for (i, received_messages) in [&received_messages1, &received_messages2, &received_messages3].iter().enumerate() {
        if let Ok(messages) = received_messages.lock() {
            assert!(!messages.is_empty(), "Subscriber {} did not receive message", i + 1);
            assert_eq!(messages[0], message, "Subscriber {} received wrong message", i + 1);
        } else {
            panic!("Failed to access received messages for subscriber {}", i + 1);
        }
    }
    
    publisher.stop().unwrap();
    subscriber1.stop().unwrap();
    subscriber2.stop().unwrap();
    subscriber3.stop().unwrap();
}

#[test]
fn test_udp_large_message_transfer() {
    // Test large message transfer over UDP
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let port = find_available_udp_port();
    let channel = format!("aeron:udp?endpoint=localhost:{}", port);
    
    let (mut subscriber, received_messages) = setup_test_subscriber(&context_dir, &channel, 4005).unwrap();
    let mut publisher = setup_test_publisher(&context_dir, &channel, 4005).unwrap();
    
    // Create a large message (50KB)
    let large_message_size = 50 * 1024;
    let large_message = vec![b'X'; large_message_size];
    
    let result = publisher.send(&large_message, &channel, 4005);
    assert!(result.is_ok());
    
    std::thread::sleep(Duration::from_millis(1000));
    
    if let Ok(messages) = received_messages.lock() {
        assert!(!messages.is_empty(), "No messages were received");
        assert_eq!(messages[0].len(), large_message_size, "Message size mismatch");
        assert_eq!(messages[0], large_message, "Large message content mismatch");
    } else {
        panic!("Failed to access received messages");
    }
    
    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_udp_high_frequency_messages() {
    // Test high-frequency message sending over UDP
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let port = find_available_udp_port();
    let channel = format!("aeron:udp?endpoint=localhost:{}", port);
    
    let (mut subscriber, received_messages) = setup_test_subscriber(&context_dir, &channel, 4006).unwrap();
    let mut publisher = setup_test_publisher(&context_dir, &channel, 4006).unwrap();
    
    let message_count = 100;
    let messages: Vec<Vec<u8>> = (0..message_count)
        .map(|i| format!("Message {}", i).into_bytes())
        .collect();
    
    // Send messages rapidly
    for message in &messages {
        publisher.send(message, &channel, 4006).unwrap();
        thread::sleep(Duration::from_micros(100)); // Very small delay
    }
    
    std::thread::sleep(Duration::from_millis(2000));
    
    if let Ok(received) = received_messages.lock() {
        assert!(received.len() >= message_count * 9 / 10, "Expected at least 90% of messages, got {}", received.len());
        
        // Verify message ordering (most should be in order)
        let mut in_order_count = 0;
        for (i, received_msg) in received.iter().take(message_count.min(received.len())).enumerate() {
            if received_msg == &messages[i] {
                in_order_count += 1;
            }
        }
        assert!(in_order_count >= message_count * 8 / 10, "Too many messages out of order");
    } else {
        panic!("Failed to access received messages");
    }
    
    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_udp_network_partition_simulation() {
    // Test behavior when network partition occurs
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let port = find_available_udp_port();
    let channel = format!("aeron:udp?endpoint=localhost:{}", port);
    
    let (mut subscriber, received_messages) = setup_test_subscriber(&context_dir, &channel, 4007).unwrap();
    let mut publisher = setup_test_publisher(&context_dir, &channel, 4007).unwrap();
    
    // Send initial message
    let message1 = b"Message before partition";
    publisher.send(message1, &channel, 4007).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    
    // Simulate network partition by stopping subscriber
    subscriber.stop().unwrap();
    std::thread::sleep(Duration::from_millis(100));
    
    // Try to send message during partition
    let message2 = b"Message during partition";
    let result = publisher.send(message2, &channel, 4007);
    assert!(result.is_ok()); // Publisher should still accept messages
    
    // Restart subscriber
    let (mut new_subscriber, new_received_messages) = setup_test_subscriber(&context_dir, &channel, 4007).unwrap();
    
    // Send message after restart
    let message3 = b"Message after restart";
    publisher.send(message3, &channel, 4007).unwrap();
    std::thread::sleep(Duration::from_millis(500));
    
    // Check original subscriber (should have only first message)
    if let Ok(messages) = received_messages.lock() {
        assert_eq!(messages.len(), 1, "Original subscriber should have only 1 message");
        assert_eq!(messages[0], message1, "Original subscriber message mismatch");
    }
    
    // Check new subscriber (should have last two messages)
    if let Ok(messages) = new_received_messages.lock() {
        assert_eq!(messages.len(), 2, "New subscriber should have only 2 message");
        assert_eq!(messages[0], message2, "New subscriber message mismatch");
        assert_eq!(messages[1], message3, "New subscriber message mismatch");
    }
    
    publisher.stop().unwrap();
    new_subscriber.stop().unwrap();
}

#[test]
fn test_udp_message_fragmentation() {
    // Test UDP message fragmentation with very large messages
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let port = find_available_udp_port();
    let channel = format!("aeron:udp?endpoint=localhost:{}", port);
    
    let (mut subscriber, received_messages) = setup_test_subscriber(&context_dir, &channel, 4009).unwrap();
    let mut publisher = setup_test_publisher(&context_dir, &channel, 4009).unwrap();
    
    // Create a very large message that will likely be fragmented
    let large_message_size = 100 * 1024; // 100KB
    let large_message = vec![b'F'; large_message_size];
    
    let result = publisher.send(&large_message, &channel, 4009);
    assert!(result.is_ok());
    
    std::thread::sleep(Duration::from_millis(2000));
    
    if let Ok(messages) = received_messages.lock() {
        assert!(!messages.is_empty(), "No messages were received");
        assert_eq!(messages[0].len(), large_message_size, "Fragmented message size mismatch");
        assert_eq!(messages[0], large_message, "Fragmented message content mismatch");
    } else {
        panic!("Failed to access received messages");
    }
    
    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_udp_concurrent_publishers_subscribers() {
    // Test multiple concurrent publishers and subscribers
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let port = find_available_udp_port();
    let channel = format!("aeron:udp?endpoint=localhost:{}", port);
    
    // Create multiple subscribers
    let mut subscribers = Vec::new();
    let mut received_messages = Vec::new();
    
    for _ in 0..3 {
        let (subscriber, messages) = setup_test_subscriber(&context_dir, &channel, 4010).unwrap();
        subscribers.push(subscriber);
        received_messages.push(messages);
    }
    
    // Create multiple publishers
    let mut publishers = Vec::new();
    for _ in 0..2 {
        let publisher = setup_test_publisher(&context_dir, &channel, 4010).unwrap();
        publishers.push(publisher);
    }
    
    // Send messages from all publishers
    let test_messages = [
        b"Message from publisher 1".to_vec(),
        b"Message from publisher 2".to_vec(),
        b"Another message from publisher 1".to_vec(),
        b"Another message from publisher 2".to_vec(),
    ];
    
    for (i, message) in test_messages.iter().enumerate() {
        let publisher_idx = i % publishers.len();
        publishers[publisher_idx].send(message, &channel, 4010).unwrap();
        thread::sleep(Duration::from_millis(50));
    }
    
    std::thread::sleep(Duration::from_millis(1000));
    
    // Verify all subscribers received messages
    for (subscriber_idx, received_messages) in received_messages.iter().enumerate() {
        if let Ok(messages) = received_messages.lock() {
            assert!(!messages.is_empty(), "Subscriber {} received no messages", subscriber_idx);
            assert!(messages.len() >= 2, "Subscriber {} received too few messages", subscriber_idx);
        }
    }
    
    // Cleanup
    for mut publisher in publishers {
        publisher.stop().unwrap();
    }
    for mut subscriber in subscribers {
        subscriber.stop().unwrap();
    }
}

#[test]
fn test_udp_error_handling() {
    // Test UDP error handling scenarios
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    
    // Test with invalid endpoint
    let invalid_channel = "aeron:udp?endpoint=invalid-host:12345";
    let received_messages = Arc::new(Mutex::new(Vec::new()));
    let fragment_assembler = create_test_fragment_assembler(received_messages);
    
    let mut subscriber = AeronSubscriber::new(&context_dir, fragment_assembler).unwrap();
    let _result = subscriber.add_subscription(invalid_channel, 4011);
    // This might succeed or fail depending on Aeron's validation, but shouldn't panic
    subscriber.stop().unwrap();
    
    // Test with very high port number
    let high_port_channel = "aeron:udp?endpoint=localhost:65535";
    let received_messages2 = Arc::new(Mutex::new(Vec::new()));
    let fragment_assembler2 = create_test_fragment_assembler(received_messages2);
    
    let mut subscriber2 = AeronSubscriber::new(&context_dir, fragment_assembler2).unwrap();
    let _result2 = subscriber2.add_subscription(high_port_channel, 4012);
    // This might succeed or fail depending on Aeron's validation, but shouldn't panic
    subscriber2.stop().unwrap();
}

#[test]
fn test_udp_performance_characteristics() {
    // Test UDP performance characteristics
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let port = find_available_udp_port();
    let channel = format!("aeron:udp?endpoint=localhost:{}", port);
    
    let (mut subscriber, received_messages) = setup_test_subscriber(&context_dir, &channel, 4013).unwrap();
    let mut publisher = setup_test_publisher(&context_dir, &channel, 4013).unwrap();
    
    let message_count = 50;
    let start_time = std::time::Instant::now();
    
    // Send messages rapidly
    for i in 0..message_count {
        let message = format!("Performance test message {}", i).into_bytes();
        publisher.send(&message, &channel, 4013).unwrap();
    }
    
    std::thread::sleep(Duration::from_millis(1000));
    
    let end_time = std::time::Instant::now();
    let duration = end_time.duration_since(start_time);
    
    if let Ok(messages) = received_messages.lock() {
        let received_count = messages.len();
        let throughput = received_count as f64 / duration.as_secs_f64();
        
        println!("UDP Performance: {} messages in {:?} ({:.2} msg/sec)", 
                 received_count, duration, throughput);
        
        assert!(received_count >= message_count * 8 / 10, 
                "Expected at least 80% message delivery, got {}", received_count);
    }
    
    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}

#[test]
fn test_udp_mixed_message_sizes() {
    // Test UDP with mixed message sizes
    let context_dir = CString::new("/tmp/aeron-test").expect("Failed to create CString");
    let port = find_available_udp_port();
    let channel = format!("aeron:udp?endpoint=localhost:{}", port);
    
    let (mut subscriber, received_messages) = setup_test_subscriber(&context_dir, &channel, 4014).unwrap();
    let mut publisher = setup_test_publisher(&context_dir, &channel, 4014).unwrap();
    
    let test_messages: Vec<Vec<u8>> = vec![
        b"Small message".to_vec(),
        vec![b'M'; 1024], // 1KB
        vec![b'L'; 10 * 1024], // 10KB
        b"Another small message".to_vec(),
        vec![b'X'; 50 * 1024], // 50KB
        b"Final small message".to_vec(),
    ];
    
    for message in &test_messages {
        publisher.send(message, &channel, 4014).unwrap();
        thread::sleep(Duration::from_millis(100));
    }
    
    std::thread::sleep(Duration::from_millis(2000));
    
    if let Ok(messages) = received_messages.lock() {
        assert_eq!(messages.len(), test_messages.len(), "Message count mismatch");
        
        for (i, (received, expected)) in messages.iter().zip(test_messages.iter()).enumerate() {
            assert_eq!(received, expected, "Message {} content mismatch", i);
        }
    } else {
        panic!("Failed to access received messages");
    }
    
    publisher.stop().unwrap();
    subscriber.stop().unwrap();
}
