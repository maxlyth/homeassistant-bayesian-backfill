# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [0.3.0] - 2026-03-27

### Fixed
- HA event loop no longer blocks during backfill computation. The Bayesian evaluation loop
  (Jinja2 rendering, probability calculation, attribute building) is now executed in a
  `@pyscript_executor` thread via `core.compute_backfill_rows()`, freeing the event loop
  for the full duration of the computation phase. The previous `await task.sleep(0)` yield
  approach was insufficient — each 500-row batch still monopolised the event loop.

### Changed
- `core.compute_backfill_rows()` added: encapsulates event timestamp collection, Jinja2
  environment setup, template compilation, the main evaluation loop, and warning generation.
  Returns a plain dict; no pyscript dependencies.
- `__init__.py`: `_backfill_single_bayesian` delegates computation to `_compute_backfill_rows`
  (`@pyscript_executor` wrapper). Removed inline computation loop and `await task.sleep(0)` yield.
- Template compile errors and debug messages are now returned from `compute_backfill_rows`
  and logged by the orchestration layer.

[0.3.0]: https://github.com/maxlyth/homeassistant-bayesian-backfill/compare/v0.2.0...v0.3.0

## [0.2.0] - 2026-03-25

### Changed
- Restructured from single `backfill_bayesian.py` to pyscript app package (`backfill_bayesian/`)
- Pure Python logic extracted to `backfill_bayesian/core.py` (no pyscript dependencies — fully testable in isolation)
- Pyscript orchestration isolated in `backfill_bayesian/__init__.py`
- Install path changed from `pyscript/backfill_bayesian.py` to `pyscript/apps/backfill_bayesian/`
- Requires `pyscript.apps.backfill_bayesian: {}` in `configuration.yaml`
- Version in `pyproject.toml` corrected to `0.2.0` (was incorrectly set to `1.0.0`)

### Fixed
- CI: excluded `backfill_bayesian` from setuptools auto-discovery to prevent byte-compile failures on pyscript-specific syntax in `__init__.py`

[0.2.0]: https://github.com/maxlyth/homeassistant-bayesian-backfill/compare/v0.1.0...v0.2.0

## [0.1.0] - 2026-03-25

### Added
- Event-driven computation at each observation entity state change (no fixed interval)
- Full attribute reconstruction: `probability`, `occurred_observation_entities`, `observations` (with `observed: false` on inactive)
- Service response visible in Developer Tools (diff summary, accuracy warnings, timing estimate, sample data)
- Batched DB writes (500 rows per commit) with yield to prevent blocking HA
- Dry run mode with diff against existing DB states
- Template evaluation with historical `now()`, `states()`, `state_attr()`, and astronomical `sun.sun` elevation
- Support for UI-created and YAML-defined Bayesian sensors
- Glob patterns for target_entity_id (e.g. `binary_sensor.*`)
- Idempotent re-runs (backfill rows tagged with `origin_idx=2`, only those are replaced)

[0.1.0]: https://github.com/maxlyth/homeassistant-bayesian-backfill/releases/tag/v0.1.0
