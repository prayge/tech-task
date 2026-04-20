"""Pytest runner for the Databricks VS Code extension.

Follows the canonical Databricks recipe
(docs.databricks.com/aws/en/dev-tools/vscode-ext/pytest):
chdir to this file's dir, disable .pyc writes, delegate to pytest.main.

Adds a small StageReporter plugin that logs before/during/after counts so
every run surfaces proposed vs realised test results in the terminal.

Run via .vscode/launch.json "Unit Tests (on Databricks)" (F5), or from CLI:
    python pytest_databricks.py tests
"""
import os
import sys
import logging

import pytest


# Docs: "Skip writing .pyc files to the bytecode cache on the cluster."
sys.dont_write_bytecode = True

# Docs: chdir so pytest discovers tests relative to this file.
os.chdir(os.path.dirname(os.path.realpath(__file__)))


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("colibri.tests")


class StageReporter:
    """Logs [BEFORE] proposed, [DURING] each outcome, [AFTER] realised totals."""

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
        if report.passed:
            self.passed += 1
        elif report.failed:
            self.failed += 1
        elif report.skipped:
            self.skipped += 1
        log.info(
            f"[DURING] {report.outcome.upper():<6} {report.nodeid}  ({report.duration:.2f}s)"
        )

    def pytest_sessionfinish(self, session, exitstatus):
        realised = self.passed + self.failed + self.skipped
        log.info("=" * 70)
        log.info(
            f"[AFTER] proposed={self.proposed} realised={realised} "
            f"passed={self.passed} failed={self.failed} skipped={self.skipped} "
            f"exit={exitstatus}"
        )
        log.info("=" * 70)


# Default target: tests/ directory. Callers may pass explicit pytest args.
args = sys.argv[1:] or ["tests", "-v", "--tb=short"]
retcode = pytest.main(args, plugins=[StageReporter()])
sys.exit(retcode)
