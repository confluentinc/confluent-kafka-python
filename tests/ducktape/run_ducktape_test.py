#!/usr/bin/env python3
"""
Simple test runner for Ducktape Producer tests
"""
import sys
import os
import subprocess
import tempfile

try:
    import ducktape
except ImportError as e:
    print("ERROR: ducktape is not installed or not importable.")
    print(f"Import error: {e}")
    print("Install it with: pip install ducktape")
    sys.exit(1)


def create_cluster_config():
    """Create a simple cluster configuration for local testing"""
    cluster_config = {
        "cluster_type": "ducktape.cluster.localhost.LocalhostCluster",
        "num_nodes": 3
    }
    return cluster_config


def create_test_config():
    """Create test configuration file"""
    import time
    timestamp = int(time.time())
    config = {
        "ducktape_dir": os.path.dirname(os.path.abspath(__file__)),
        "results_dir": os.path.join(tempfile.gettempdir(), f"ducktape_results_{timestamp}")
    }
    return config


def main():
    """Run the ducktape producer test"""
    print("Confluent Kafka Python - Ducktape Producer Test Runner")
    print("=" * 60)

    try:
        print(f"Using ducktape version: {ducktape.__version__}")
    except AttributeError:
        # Some ducktape versions don't have __version__, try alternative methods
        try:
            import pkg_resources
            version = pkg_resources.get_distribution('ducktape').version
            print(f"Using ducktape version: {version}")
        except:
            print("Using ducktape version: unknown")

    # Check if confluent_kafka is available
    try:
        import confluent_kafka
        print(f"Using confluent-kafka version: {confluent_kafka.version()}")
    except ImportError:
        print("ERROR: confluent_kafka is not installed.")
        print("Install it with: pip install confluent-kafka")
        return 1

    # Get test file path (ducktape expects file paths, not module paths)
    test_dir = os.path.dirname(os.path.abspath(__file__))
    test_file = os.path.join(test_dir, "test_producer.py")

    if not os.path.exists(test_file):
        print(f"ERROR: Test file not found: {test_file}")
        return 1

    # Create results directory
    results_dir = os.path.join(tempfile.gettempdir(), "ducktape_results")
    os.makedirs(results_dir, exist_ok=True)

    print(f"Test file: {test_file}")
    print(f"Results directory: {results_dir}")
    print()

    # Build ducktape command using improved subprocess approach
    cmd = [
        sys.executable, "-m", "ducktape",
        "--debug",  # Enable debug output
        "--results-root", results_dir,
        "--cluster", "ducktape.cluster.localhost.LocalhostCluster",
        "--default-num-nodes", "1",  # Reduced since we don't need nodes for services
        test_file
    ]

    # Add specific test if provided as argument
    if len(sys.argv) > 1:
        test_method = sys.argv[1]
        cmd[-1] = f"{test_file}::{test_method}"
        print(f"Running specific test: {test_method}")
    else:
        print("Running all producer tests")

    print("Command:", " ".join(cmd))
    print()

    # Run the test
    try:
        result = subprocess.run(cmd)
        return result.returncode
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        return 1
    except Exception as e:
        print(f"Error running test: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
