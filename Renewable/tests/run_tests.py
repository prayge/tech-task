# Databricks notebook source
# MAGIC %md
# MAGIC # Colibri test runner
# MAGIC
# MAGIC Invoked by the `colibri-tests` job defined in `databricks.yml`.
# MAGIC Shells out to `pytest` against the bundle-uploaded `tests/` folder so
# MAGIC adding new `test_*.py` files requires no job wiring.

# COMMAND ----------

import os
import sys

import pytest


# The notebook lives in the bundle's workspace.file_path under `tests/`, so
# siblings (`conftest.py`, `test_bronze.py`, ...) are the test target and the
# parent is the pyproject rootdir.
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
notebook_path = ctx.notebookPath().get()
tests_dir = "/Workspace" + os.path.dirname(notebook_path)
root_dir = os.path.dirname(tests_dir)

# `transformations/` is a sibling of `tests/` — make it importable without
# building and installing the wheel.
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

exit_code = pytest.main(["-q", "--rootdir", root_dir, tests_dir])

if exit_code != 0:
    raise SystemExit(f"pytest failed with exit code {exit_code}")
