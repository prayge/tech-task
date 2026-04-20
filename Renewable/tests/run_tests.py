"""Test runner for Databricks job execution.

Installs pytest if needed, runs all tests in the tests directory, and reports
results. Exit code 0 indicates all tests passed, non-zero indicates failures.

Usage:
    python tests/run_tests.py

Can be scheduled as a Databricks job task. Requires:
- Databricks Connect configured (or runs on cluster)
- Unity Catalog volume /Volumes/colibri/test/data with test fixtures
- transformations package accessible
"""
import sys
import subprocess
from pathlib import Path


def ensure_pytest():
    """Install pytest if not available."""
    try:
        import pytest
        print(f"pytest {pytest.__version__} already installed")
    except ImportError:
        print("Installing pytest...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pytest"])
        import pytest
        print(f"pytest {pytest.__version__} installed successfully")


def run_tests():
    """Run all tests in the tests directory."""
    import pytest
    
    # Get the tests directory (parent of this file)
    tests_dir = Path(__file__).parent
    
    print(f"\n{'='*70}")
    print(f"Running tests from: {tests_dir}")
    print(f"{'='*70}\n")
    
    # Run pytest with verbose output
    # -v: verbose
    # -s: show print statements
    # --tb=short: shorter traceback format
    exit_code = pytest.main([
        str(tests_dir),
        "-v",
        "-s",
        "--tb=short",
    ])
    
    print(f"\n{'='*70}")
    if exit_code == 0:
        print("✓ All tests passed!")
    else:
        print(f"✗ Tests failed with exit code: {exit_code}")
    print(f"{'='*70}\n")
    
    return exit_code


if __name__ == "__main__":
    ensure_pytest()
    exit_code = run_tests()
    sys.exit(exit_code)
