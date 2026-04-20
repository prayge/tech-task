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
import inspect
import logging
import subprocess
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("colibri.tests")


def _this_dir():
    # spark_python_task is run via a compiled-string entrypoint that leaves
    # __file__ unbound. Fall back to the current source file via inspect.
    try:
        return Path(__file__).parent
    except NameError:
        return Path(inspect.getsourcefile(lambda: None)).parent


def ensure_pytest():
    try:
        import pytest
        log.info(f"pytest {pytest.__version__} already installed")
    except ImportError:
        log.info("installing pytest...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pytest"])
        import pytest
        log.info(f"pytest {pytest.__version__} installed")


class StageReporter:
    """Pytest plugin that logs before/during/after with proposed/realised counts."""

    def __init__(self):
        self.proposed = 0
        self.passed = 0
        self.failed = 0
        self.skipped = 0

    def pytest_collection_finish(self, session):
        self.proposed = len(session.items)
        log.info(f"[BEFORE] proposed={self.proposed} tests collected")
        for item in session.items:
            log.info(f"[BEFORE]   - {item.nodeid}")

    def pytest_runtest_logstart(self, nodeid, location):
        log.info(f"[DURING] start  {nodeid}")

    def pytest_runtest_logreport(self, report):
        if report.when != "call" and not (report.when == "setup" and report.skipped):
            return
        outcome = report.outcome.upper()
        if report.passed:
            self.passed += 1
        elif report.failed:
            self.failed += 1
        elif report.skipped:
            self.skipped += 1
        log.info(f"[DURING] {outcome:<6} {report.nodeid}  ({report.duration:.2f}s)")

    def pytest_sessionfinish(self, session, exitstatus):
        realised = self.passed + self.failed + self.skipped
        log.info("=" * 70)
        log.info(
            f"[AFTER] proposed={self.proposed} realised={realised} "
            f"passed={self.passed} failed={self.failed} skipped={self.skipped} "
            f"exit={exitstatus}"
        )
        log.info("=" * 70)


def run_tests():
    import pytest

    tests_dir = _this_dir()
    log.info(f"running tests from: {tests_dir}")

    return pytest.main(
        [str(tests_dir), "-v", "-s", "--tb=short"],
        plugins=[StageReporter()],
    )


if __name__ == "__main__":
    ensure_pytest()
    exit_code = run_tests()
    sys.exit(exit_code)
