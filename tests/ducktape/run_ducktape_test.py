#!/usr/bin/env python3
"""
Simple test runner for Ducktape Producer tests
"""
import sys
import os
import subprocess
import tempfile
import json

# Add the project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, project_root)

def create_cluster_config():
    """Create a simple cluster configuration for local testing"""
    cluster_config = {
        "cluster_type": "ducktape.cluster.localhost.LocalhostCluster",
        "num_nodes": 3
    }
    return cluster_config

def create_test_config():
    """Create test configuration file"""
    config = {
        "ducktape_dir": os.path.dirname(os.path.abspath(__file__)),
        "results_dir": os.path.join(tempfile.gettempdir(), "ducktape_results")
    }
    return config

def main():
    """Run the ducktape producer test"""
    print("Confluent Kafka Python - Ducktape Producer Test Runner")
    print("=" * 60)
    
    # Check if ducktape is installed
    try:
        import ducktape
        print(f"Using ducktape version: {ducktape.__version__}")
    except ImportError:
        print("ERROR: ducktape is not installed.")
        print("Install it with: pip install ducktape")
        return 1
        
    # Check if confluent_kafka is available
    try:
        import confluent_kafka
        print(f"Using confluent-kafka version: {confluent_kafka.version()}")
    except ImportError:
        print("ERROR: confluent_kafka is not installed.")
        print("Install it with: pip install confluent-kafka")
        return 1
    
    # Get test directory and file
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
    
    # Build ducktape command
    cmd = [
        "ducktape",
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
    
    # Set up environment with proper Python path
    env = os.environ.copy()
    env['PYTHONPATH'] = project_root
    
    # Run the test
    try:
        result = subprocess.run(cmd, cwd=project_root, env=env)
        return result.returncode
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        return 1
    except Exception as e:
        print(f"Error running test: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
