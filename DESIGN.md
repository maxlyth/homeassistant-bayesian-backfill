# Design notes

## Problem

When a Home Assistant Bayesian binary sensor is created or its observation probabilities are modified, HA only records values going forward. This means:

- Newly created sensors have no historical probability data in the statistics database.
- Sensors with modified `prob_given_true`/`prob_given_false` values have history that reflects the old model, not the current one.

This service reconstructs the correct probability history retroactively from the raw state data already present in the recorder database.

---

## Design decisions

| Decision | Choice | Rationale |
|---|---|---|
| Trigger | Manual service call only | HA emits no reliable event on YAML or UI config change; backfill can take minutes |
| Stored value | Probability 0.0–1.0 numeric statistic | Preserves full nuance vs binary on/off |
| Default `start_ts` | Earliest observation entity state in `states` table | Covers both the new-sensor and post-modification cases automatically |
| Default `end_ts` | Always fill to now | HA never writes Bayesian probability to `statistics` itself — nothing to protect; re-running after modification naturally overwrites old values |
| Time-dependent templates | Evaluate with historical `now()` | `now().hour`/`now().weekday()` naturally reflect the historical timestamp, giving correct time-of-day patterns |
| `sun.sun` elevation | Compute astronomically | `sun.sun` is not recorded in the `states` table; implemented as a ~20-line pure Python formula using `math` + lat/lon from `core.config` |
| Template entity extraction | Regex scan | Extract `entity_id`s from `states()`/`state_attr()` calls in template strings; unresolved entities return `"unknown"` gracefully |
| Path resolution | `hass.config.config_dir` at runtime | Works for any HA installation regardless of where the config directory lives |
| YAML sensor discovery | Recursive two-pass scan | String-match for `platform: bayesian` first, then `yaml.safe_load` only on matching files — avoids parsing every YAML file |
| Modification semantics | Full history recomputed with current config | One consistent probability model applied retroactively across the entire window |
| Glob support | `fnmatch` on entity registry | `binary_sensor.*` backfills all Bayesian sensors in one call |
| Dry run | Per-batch summary + `debug=True` | `debug=True` adds a per-observation breakdown for the first 10 timestamps to make it easy to validate before writing |
| Concurrency guard | `task.unique("backfill_bayesian_sensor")` | Prevents parallel SQLite scans from the same pyscript service |

---

## Architecture

### Config source lookup

Sensor configuration is resolved at runtime — no hardcoded paths or sensor names:

```
target_entity_id
  → core.entity_registry        find unique_id and config_entry_id
  → if config_entry_id present:  load from core.config_entries (UI-created sensor)
  → else:                        strip "bayesian-" prefix from unique_id,
                                 recursively scan YAML files             (YAML-defined sensor)
```

### State reconstruction

All observation entity histories are loaded in a single bulk SQL query against the `states` + `states_meta` tables, with a 1-day lookback before `start_ts` to seed forward-fill correctly. Per-timestamp lookups then use `bisect.bisect_right` for O(log n) forward-fill.

### Jinja2 mock environment

Template observations are evaluated against a Jinja2 environment with `states()`, `state_attr()`, and `now()` overridden to return historical values at the target timestamp. `state_attr('sun.sun', 'elevation')` is special-cased to call `_solar_elevation(ts, lat, lon)` since `sun.sun` is not recorded in the `states` table.

### Bayes update formula

```
p = prior
for each observation:
    if active:   p = (p × p_true)  / (p × p_true  + (1−p) × p_false)
    if inactive: p = (p × (1−p_true)) / (p × (1−p_true) + (1−p) × (1−p_false))
    if unknown:  skip (entity had no state at this timestamp)
return clamp(p, 0.0001, 0.9999)
```

### Why not write directly to the database?

Direct SQLite writes bypass HA's in-memory recorder cache, which causes duplicate rows until the next restart. `recorder.import_statistics` is the official upsert API — it is safe, idempotent, and cache-aware.

---

## Key functions

| Function | Purpose |
|---|---|
| `_solar_elevation(ts, lat, lon)` | Pure Python solar elevation from timestamp + lat/lon |
| `_load_location(config_dir)` | Read lat/lon from `.storage/core.config` |
| `_get_bayesian_entity_ids(pattern, config_dir)` | Expand glob pattern against entity registry |
| `_load_bayesian_config(entity_id, config_dir)` | Load config from UI storage or YAML scan |
| `_get_bayesian_window_start(entity_ids, db_path)` | `MIN(last_updated_ts)` across observation entities |
| `_load_state_timelines(entity_ids, ...)` | Bulk-load state histories via single SQL query |
| `_get_state_at(timeline, ts)` | Forward-fill state lookup using `bisect` |
| `_get_attr_at(timeline, ts, attr)` | Forward-fill attribute lookup using `bisect` |
| `_build_jinja2_env(timelines, lat, lon)` | Mock Jinja2 env with historical `states()`, `state_attr()`, `now()` |
| `_evaluate_observation(obs, ...)` | Dispatch by platform: `state` / `numeric_state` / `template` |
| `_compute_bayesian_probability(prior, obs, results)` | Iterative Bayes update |
| `_backfill_single_bayesian(...)` | Core logic for one sensor |
| `backfill_bayesian_sensor(...)` | `@service` orchestrator; handles glob expansion |

---

## Edge cases

| Case | Handling |
|---|---|
| Entity not in `states_meta` | Empty timeline → observation always skipped |
| Numeric state `"unavailable"` | `float()` raises `ValueError` → observation skipped |
| `sun.sun` elevation | Computed astronomically; never queried from `states` table |
| Template render error | Returns `False` (conservative — treats observation as inactive) |
| Dependent Bayesian sensors | HA records their `on`/`off` state in `states` → handled naturally |
| Glob matches 0 sensors | Logs error listing the pattern and instructs user to check registry |
| Config not found for entity | Logs error showing which registry entry and files were checked |
| YAML nested package format | Top-level dict detected → each value list is searched for `platform: bayesian` |
