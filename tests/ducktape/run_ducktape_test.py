#!/usr/bin/env python3
"""
Unified test runner for Ducktape Producer and Consumer tests
"""
import sys
import os
import subprocess
import tempfile
import argparse
from datetime import datetime

import ducktape


def get_test_info(test_type):
    """Get test file and description based on test type"""
    test_info = {
        'producer': {
            'file': 'test_producer.py',
            'description': 'Producer Benchmark Tests'
        },
        'consumer': {
            'file': 'test_consumer.py',
            'description': 'Consumer Benchmark Tests'
        },
        'producer_sr': {
            'file': 'test_producer_with_schema_registry.py',
            'description': 'Producer with Schema Registry Tests'
        },
        'transactions': {
            'file': 'test_transactions.py',
            'description': 'Transactional Producer and Consumer Tests'
        }
    }
    return test_info.get(test_type)


def run_single_test_type(args):
    """Run a single test type"""
    test_info = get_test_info(args.test_type)

    # Header
    print(f"Confluent Kafka Python - {test_info['description']}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 70)

    try:
        print(f"Using ducktape version: {ducktape.__version__}")
    except AttributeError:
        # Some ducktape versions don't have __version__, try alternative methods
        try:
            import pkg_resources
            version = pkg_resources.get_distribution('ducktape').version
            print(f"Using ducktape version: {version}")
        except Exception:
            print("Using ducktape version: unknown")

    # Check if confluent_kafka is available
    try:
        import confluent_kafka
        print(f"Using confluent-kafka version: {confluent_kafka.version()}")
    except ImportError:
        print("ERROR: confluent_kafka is not installed.")
        print("Install it with: pip install confluent-kafka")
        return 1

    # Get test file path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    test_file = os.path.join(script_dir, test_info['file'])
    project_root = os.path.dirname(os.path.dirname(script_dir))

    if not os.path.exists(test_file):
        print(f"ERROR: Test file not found: {test_file}")
        return 1

    # Set working directory to project root (needed for imports)
    os.chdir(project_root)

    # Create results directory
    results_dir = os.path.join(tempfile.gettempdir(), "ducktape_results")
    os.makedirs(results_dir, exist_ok=True)

    print(f"\nWorking directory: {os.getcwd()}")
    print(f"Test file: {test_file}")
    print(f"Results directory: {results_dir}")

    # Build ducktape command
    cmd = [
        sys.executable, "-m", "ducktape",
        "--results-root", results_dir,
        "--cluster", "ducktape.cluster.localhost.LocalhostCluster",
        "--default-num-nodes", "1"
    ]

    if args.debug:
        cmd.append("--debug")

    # Add test file
    if args.test_method:
        cmd.append(f"{test_file}::{args.test_method}")
        print(f"\nRunning specific test: {args.test_method}")
    else:
        cmd.append(test_file)
        print(f"\nRunning all {args.test_type} tests...")

    # Set up environment with PYTHONPATH
    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd()

    print(f"Command: PYTHONPATH=. {' '.join(cmd[2:])}")  # Skip python -m ducktape for readability
    print("-" * 50)

    # Run the test
    try:
        result = subprocess.run(cmd, env=env)

        # Summary
        print("\n" + "=" * 50)
        if result.returncode == 0:
            print("All tests completed successfully!")
        else:
            print("Tests failed. Check the output above for details.")

        return result.returncode

    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        return 1
    except Exception as e:
        print(f"Error running test: {e}")
        return 1


def run_all_tests(args):
    """Run all available test types"""
    test_types = ['producer', 'consumer', 'producer_sr']
    overall_success = True

    print("Confluent Kafka Python - All Ducktape Tests")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 70)

    for test_type in test_types:
        print(f"\n{'='*20} Running {test_type.upper()} Tests {'='*20}")

        # Create a new args object for this test type
        test_args = argparse.Namespace(
            test_type=test_type,
            test_method=args.test_method,
            debug=args.debug
        )

        # Run the specific test type
        result = run_single_test_type(test_args)
        if result != 0:
            overall_success = False
            print(f"\n❌ {test_type.upper()} tests failed!")
        else:
            print(f"\n✅ {test_type.upper()} tests passed!")

    print(f"\n{'='*70}")
    if overall_success:
        print("🎉 All tests completed successfully!")
        return 0
    else:
        print("💥 Some tests failed. Check the output above for details.")
        return 1


def main():
    """Run the ducktape test based on specified type"""
    parser = argparse.ArgumentParser(description="Confluent Kafka Python - Ducktape Test Runner")
    parser.add_argument('test_type', nargs='?', choices=['producer', 'consumer', 'producer_sr', 'transactions'],
                        help='Type of test to run (default: run all tests)')
    parser.add_argument('test_method', nargs='?',
                        help='Specific test method to run (optional)')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug output')

    args = parser.parse_args()

    # If no test_type provided, run all tests
    if args.test_type is None:
        return run_all_tests(args)

    # Run single test type
    return run_single_test_type(args)


if __name__ == "__main__":
    sys.exit(main())
