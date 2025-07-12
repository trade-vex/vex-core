#!/bin/bash

# Helper script for Docker operations

set -e

function show_help() {
    echo "Usage: $0 <command>"
    echo
    echo "Commands:"
    echo "  test-media-driver     - Test media driver independently"
    echo "  generate-sbe          - Generate SBE Rust code from ordercmd.xml"
    echo "  start-full            - Start full application (media driver + vex-core)"
    echo "  stop-all              - Stop all containers"
    echo "  clean-all             - Clean up all containers and volumes"
    echo "  logs-media-driver     - Show media driver logs"
    echo "  logs-sbe              - Show SBE generator logs"
    echo "  help                  - Show this help message"
    echo
}

function test_media_driver() {
    echo "🚀 Testing Media Driver independently..."
    docker compose -f docker-compose.media-driver.yml up --build
}

function generate_sbe() {
    echo "🔧 Generating SBE Rust code from ordercmd.xml..."
    docker compose up --build sbe-generator
    echo "✅ SBE code generated in ./target/generated/"
    echo "Generated files:"
    ls -la ./target/generated/ 2>/dev/null || echo "Directory not found - check for errors above"
}

function start_full() {
    echo "🚀 Starting full application..."
    docker compose up --build
}

function stop_all() {
    echo "🛑 Stopping all containers..."
    docker compose down || true
    docker compose -f docker-compose.media-driver.yml down || true
}

function clean_all() {
    echo "🧹 Cleaning up all containers and volumes..."
    stop_all
    docker compose down -v || true
    docker compose -f docker-compose.media-driver.yml down -v || true
    docker system prune -f
}

function logs_media_driver() {
    echo "📋 Media Driver logs:"
    docker compose -f docker-compose.media-driver.yml logs -f media-driver
}

function logs_sbe() {
    echo "📋 SBE Generator logs:"
    docker compose logs -f sbe-generator
}

case "$1" in
    test-media-driver)
        test_media_driver
        ;;
    generate-sbe)
        generate_sbe
        ;;
    start-full)
        start_full
        ;;
    stop-all)
        stop_all
        ;;
    clean-all)
        clean_all
        ;;
    logs-media-driver)
        logs_media_driver
        ;;
    logs-sbe)
        logs_sbe
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        echo "❌ Unknown command: $1"
        echo
        show_help
        exit 1
        ;;
esac 