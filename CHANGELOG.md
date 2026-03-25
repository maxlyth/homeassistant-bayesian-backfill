# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

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
