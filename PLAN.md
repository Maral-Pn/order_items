# Fix plan

Findings from a review of `code/` and `.github/workflows/cicd.yml` (tests currently
pass: `5 passed` via `.venv/bin/python -m pytest code`). Nothing here is a crash in the
happy path exercised by CI; the bugs below only surface in specific cases (running a
test file directly) or as data-correctness/coverage risk.

## 1. Bugs

- **`NameError` if a test file is run directly.** `code/test/test_base.py`,
  `code/test/test_transformer/test_transformer.py`, and
  `code/test/test_pipeline/test_mypipeline.py` each import `from unittest import
  TestCase` but call `unittest.main()` (not `TestCase`-qualified) in their
  `if __name__ == "__main__":` block. `unittest` itself is never imported, so
  `python test_transformer.py` (bypassing pytest) raises `NameError: name 'unittest'
  is not defined`. Fix: `import unittest` in each file, and drop the now-unused
  `from unittest import TestCase` import where `TestCase` itself isn't otherwise used.

- **`unit_price` schema is `LongType`.** `code/core/schema/schema_repository.py`
  defines `unit_price` (and `quantity`) as `LongType` for `SchemaEnum.ORDER_ITEM`.
  Dummy data only uses whole numbers so tests pass, but any real order with a
  fractional price (e.g. `12.50`) will silently fail `from_json` parsing (field comes
  back `null`) rather than erroring. Fix: change `unit_price` to `DoubleType` (or
  `DecimalType` if exact cents matter).

- **Misleading test class name.** `code/test/test_pipeline/test_mypipeline.py` defines
  `class TestTransformer(TestBase)` even though it tests `Pipeline`, not `Transformer`
  — same class name as the real `TestTransformer` in `test_transformer.py`. Rename to
  `TestPipeline` to avoid confusion during test discovery/reporting.

- **Dead CI lint step.** `.github/workflows/cicd.yml`'s "Lint with flake8" step has
  every `flake8` invocation commented out, so it installs flake8 and then does
  nothing — the step name is misleading. Either re-enable a real flake8 invocation or
  remove the step (and the `pip install flake8`) until linting is actually wanted.
  CLAUDE.md already documents this as a known gap; this just resolves it one way or
  the other.

## 2. Test coverage gaps

- `Transformer.getProductDataframe` has no direct unit test (only exercised
  indirectly through the pipeline).
- `Pipeline.transformProduct`, `transformJoined`, `transformTotalPrice`, `write`,
  `runPipeline`, `readOrder`, `readProduct` are untested — `test_mypipeline.py`
  currently only covers `transformOrder`.
- `Pipeline.initialiseSpark` is only tested with `master="local"`; the fallback
  `case m:` branch (non-local master string) is untested.
- No tests exist for `OutputWriter.write` (mode/partition_column/fmt handling) or
  `SchemaRepository.getSchema` (including the `NotImplementedError` for an
  unregistered `SchemaEnum`).

## 3. Minor cleanups

- GST rate `1.03` is duplicated as both `PipelineConfig.gst_rate`'s default and
  `Transformer.DEFAULT_GST_RATE`. Consider having `PipelineConfig.gst_rate` default
  from `Transformer.DEFAULT_GST_RATE` so there's one source of truth.
- `Pipeline.initialiseSpark` sets `appName("Test")` for the `"local"` branch but
  `appName("test")` (different case) for every other master — harmless but
  inconsistent.

## Suggested order of work

1. Fix the three `NameError`/naming bugs and the schema `LongType` issue (small,
   isolated changes).
2. Decide on the CI lint step (remove or re-enable) and update it.
3. Backfill the missing tests in section 2.
4. Optional cleanup items in section 3.
