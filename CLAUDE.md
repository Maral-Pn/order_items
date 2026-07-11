# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A local PySpark batch job that joins order items with product data and computes a
total price (incl. GST) per client. All input data is currently hard-coded/dummy data
served through `DummyDataSource` — there is no real data source wired up yet, but the
`DataSource` abstraction exists so one can be added without touching `Pipeline`.

## Commands

Run from the `code/` directory — there is no `code/__init__.py`, so `core` and `test`
are treated as top-level packages and imports (`from core...`, `from test.test_base import...`)
only resolve when `code/` is the working directory / on `sys.path`.

```bash
cd code

# run all tests
python -m pytest

# run a single test module / test case
python -m pytest test/test_transformer/test_transformer.py
python -m pytest test/test_pipeline/test_mypipeline.py::TestTransformer::test_transformOrder

# build the zip that gets shipped to spark-submit
zip -ru9 core.zip core/

# run the job locally (from repo root, matching README/CI)
cd ..
spark-submit --master local --py-files code/core.zip code/jobs/driver.py
```

There is no lint step in CI — a previously-commented-out flake8 step was removed rather than re-enabled.

## Architecture

Entry point: `code/jobs/driver.py` builds a `PipelineConfig`, constructs a `Pipeline`
with it (`verbose=True` so intermediate frames get printed), calls
`initialiseSpark(config.master)`, then `runPipeline()`.

`Pipeline` (`code/core/pipeline/my_pipeline.py`) is an orchestrator only — it holds no
business logic itself, just collaborators that are injected (or default-constructed if
omitted):

- **`PipelineConfig`** (`code/core/config/pipeline_config.py`) — frozen dataclass with
  every environment-specific knob: `master`, `output_path`, `partition_column`,
  `write_mode`, `gst_rate`. This is the one place to change any of those instead of
  hunting through `Pipeline`/`Transformer`.
- **`DataSource`** (`code/core/io/data_source.py`) — abstract `get_order_data()` /
  `get_product_data()`. `DummyDataSource` (`code/core/io/dummy_data_source.py`) is the
  only implementation: hard-coded Spark DataFrames where orders have an
  `order_item_list` column holding a *JSON string* array of
  `{product_id, unit_price, quantity}`, and products have a single `products` column
  holding a *JSON string* per row. `Pipeline.initialiseSpark` defaults to
  `DummyDataSource` if no `data_source` was passed into the constructor — pass a
  different `DataSource` implementation to point the same pipeline at real data.
- **`SchemaRepository.getSchema(SchemaEnum...)`** (`code/core/schema/`) — single source
  of truth for the `StructType`/`ArrayType` used to parse those JSON strings via
  `from_json`. Add a new case there (and a new `SchemaEnum` member) before parsing any
  new JSON-string column.
- **`Transformer`** (`code/core/transform/transformer.py`) — the actual Spark work, and
  intentionally free of I/O/printing side effects. `flattenOrder`/`flattenProduct` parse
  the JSON string columns with the schemas above and explode/select into flat columns;
  `joinDataframe` inner-joins on `product_id == Id`; `getReceiptDataframe` groups by
  `client_name` and sums `unit_price * quantity * gst_rate` (rate comes from
  `PipelineConfig.gst_rate`, defaulting to `Transformer.DEFAULT_GST_RATE`). `Pipeline`
  builds one `Transformer` in `initialiseSpark` and reuses it rather than constructing a
  fresh one per transform step.
- **`OutputWriter`** (`code/core/io/output_writer.py`) — writes a DataFrame given
  `path`/`partition_column`/`mode`/`fmt`; nothing about the sink is hardcoded in
  `Pipeline` anymore.

`Pipeline._show()` gates all `.show(truncate=False)` debug printing behind the
`verbose` constructor flag, so tests and non-interactive runs stay quiet by default.

Tests (`code/test/`) mirror this structure (`test_pipeline/`, `test_transformer/`) and
share `TestBase` (`code/test/test_base.py`), which spins up a local `SparkSession` per
test and provides `assertDataFrameEqual` (schema + data equality via
`pyspark.testing`). `test_transformer.py` covers `flattenOrder`, `flattenProduct`,
`joinDataframe`, and `getReceiptDataframe` (including the GST total math); no test
stubs remain.

## CI/CD

`.github/workflows/cicd.yml` runs on push/PR to `main`: installs deps from
`requirements.txt`, runs `pytest` from the repo root (pytest auto-inserts `code/` onto
`sys.path` since `code/` itself has no `__init__.py`, so this works the same as running
from inside `code/`), zips `code/core` into `core.zip`, then does a local
`spark-submit` of the driver as a smoke test.

CI is confirmed working: the last three pushes to `main` (including both the
push event and the PR's `pull_request` event for the most recent merge) have
completed successfully. If a run doesn't show up for a push, check the repo's
Actions settings first — Actions has been disabled for periods in the past.
