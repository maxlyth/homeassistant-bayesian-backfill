# homeassistant-bayesian-backfill

A pyscript service that retroactively recomputes Bayesian binary sensor state history in Home Assistant. When observation probabilities (`prob_given_true`/`prob_given_false`) are changed, or a new Bayesian sensor is created, the existing history doesn't reflect the updated model. This service reconstructs the correct probability history from raw state data already in your recorder database and writes it directly to the states table so it appears in history graphs and ApexCharts attribute-history cards.

---

## Table of contents

- [How it works](#how-it-works)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Service: backfill\_bayesian\_sensor](#service-backfill_bayesian_sensor)
- [Running the tests](#running-the-tests)

---

## How it works

1. Resolves sensor config from `core.config_entries` (UI-created sensors) or by recursively scanning your YAML files (YAML-defined sensors).
2. Bulk-loads all observation entity state histories from the `states` table in a single SQL query.
3. For each interval timestamp, evaluates every observation (`state`, `numeric_state`, `template`) against the historical states using forward-fill.
4. Template observations use a Jinja2 environment with mocked `states()`, `state_attr()`, and `now()` returning historical values. `state_attr('sun.sun', 'elevation')` is computed astronomically (pure Python, no `astral` dependency) from the lat/lon stored in your HA configuration.
5. Runs the iterative Bayes update formula and computes the resulting probability (0.0-1.0).
6. Writes synthetic state records to the `states` table with the computed `probability` attribute, so the values appear in history graphs and ApexCharts attribute-history cards.

All paths are resolved at runtime from `hass.config.config_dir`, so the script works on any HA installation regardless of where your config directory is located.

---

## Prerequisites

- Home Assistant (any recent version with the `recorder` integration enabled -- it is on by default)
- The [pyscript custom component](https://github.com/custom-components/pyscript) installed and enabled
- `jinja2` -- bundled with Home Assistant, no separate install needed

---

## Installation

### Step 1 -- Install pyscript

**Via HACS (recommended)**

1. Open HACS in your HA sidebar.
2. Go to **Integrations > Explore & Download Repositories**.
3. Search for **pyscript** and install it.
4. Restart Home Assistant when prompted.

**Manually**

Download the latest release from the [pyscript releases page](https://github.com/custom-components/pyscript/releases) and copy the `pyscript` folder into `<config>/custom_components/`.

### Step 2 -- Enable pyscript in your configuration

Add the following to your `configuration.yaml`. The `allow_all_imports` flag is required because the script imports Python standard-library modules (`sqlite3`, `json`, `re`, etc.).

```yaml
pyscript:
  allow_all_imports: true
```

If you already have a `pyscript:` section, add `allow_all_imports: true` inside it.

### Step 3 -- Copy the script

Copy `backfill_bayesian.py` into the `pyscript/` directory inside your HA config folder. The directory is created automatically the first time pyscript runs, but you can create it manually if it does not exist yet:

```
<config>/
  pyscript/
    backfill_bayesian.py
```

The typical config directory location depends on your installation method:

| Install type | Config directory |
|---|---|
| Home Assistant OS | `/homeassistant/` (accessible via the Samba or SSH add-on) |
| Home Assistant Container | Whatever you mapped to `/config` in your `docker run` / `compose.yaml` |
| Home Assistant Core (venv) | `~/.homeassistant/` by default |

### Step 4 -- Reload pyscript

You do not need a full HA restart. In the HA UI go to:

**Developer Tools > YAML > Reload pyscript**

Or call the service directly:

```yaml
action: pyscript.reload
```

### Step 5 -- Verify

Go to **Developer Tools > Actions** and search for `pyscript.backfill_bayesian_sensor`. If it does not appear, check **Settings > System > Logs** for pyscript errors. The most common causes are:

- `allow_all_imports: true` missing from `configuration.yaml`
- A syntax error introduced by an edit to the script (HA will log the line number)
- The script placed in the wrong directory (it must be directly inside `pyscript/`, not in a subdirectory)

---

## Service: `backfill_bayesian_sensor`

### Parameters

| Field | Required | Default | Description |
|---|---|---|---|
| `target_entity_id` | yes | -- | Entity ID or glob pattern (e.g. `binary_sensor.*`) |
| `start_offset` | no | Earliest observation entity state | How far back from now to start the backfill window |
| `end_offset` | no | Now | How far back from now to end the backfill window |
| `interval_minutes` | no | `60` | Granularity in minutes |
| `dry_run` | no | `false` | Log computed values without writing to the database |
| `debug` | no | `false` | Log a per-observation breakdown for the first 10 timestamps |
| `write_history` | no | `true` | Write computed probability as a state attribute to the `states` table. Re-running is safe -- only backfill-written rows are replaced; real HA-recorded states are never touched. Records older than `purge_keep_days` are skipped. |

### Examples

```yaml
# Single sensor -- dry run with debug output to validate before writing
action: pyscript.backfill_bayesian_sensor
data:
  target_entity_id: binary_sensor.bathroom_occupied
  dry_run: true
  debug: true
```

```yaml
# All Bayesian sensors, last 7 days
action: pyscript.backfill_bayesian_sensor
data:
  target_entity_id: binary_sensor.*
  start_offset:
    days: 7
```

```yaml
# Specific time window, history only
action: pyscript.backfill_bayesian_sensor
data:
  target_entity_id: binary_sensor.kitchen_occupied
  start_offset:
    hours: 48
  end_offset:
    hours: 0
```

### Recommended workflow

1. Run with `dry_run: true` and `debug: true`. Check the HA logs -- you should see a per-observation breakdown for the first 10 timestamps and a probability value that oscillates plausibly between 0 and 1.
2. Spot-check: pick a timestamp where you know the state of the observation entities. Hand-calculate the Bayes update and compare to the logged probability.
3. Run without `dry_run`. The service is idempotent -- safe to re-run after modifying observation probabilities.
4. Check the **History** panel -- the entity's `probability` attribute should now appear for the backfilled period.

### How state history writes work

The service writes synthetic state records directly to the recorder's `states` table. Each record carries the full attribute set of the real sensor (device_class, friendly_name, observations list, etc.) with the computed `probability` value overlaid.

Key details:

- **Idempotent**: Backfill-written rows are tagged with `origin_idx=2`. Re-running deletes only those rows before reinserting, so real HA-recorded states are never affected.
- **Retention-aware**: Rows older than your `purge_keep_days` setting (read from `configuration.yaml`, or inferred from the oldest state in the database, or defaulting to 10 days) are not written.
- **State transitions**: `last_changed_ts` is only set when the binary state actually changes (on/off or off/on), matching HA's native recorder semantics.
- **Attribute deduplication**: Identical attribute JSON blobs are shared via the `state_attributes` table, avoiding storage bloat.

---

## Running the tests

The test suite runs outside of Home Assistant using pytest. It covers solar elevation, state forward-fill, Bayes update formula, observation evaluation, Jinja2 historical mocks, SQLite state loading, YAML sensor discovery, UI config_entries lookup, purge_keep_days resolution, and state history write-back -- 104 tests in total.

```bash
git clone https://github.com/maxlyth/homeassistant-bayesian-backfill.git
cd homeassistant-bayesian-backfill

python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -e ".[test]"
pytest
```

Expected output: `104 passed`.

There are also two integration tests that run against a live HA instance:

```bash
HA_URL=http://supervisor/core HA_TOKEN=$SUPERVISOR_TOKEN pytest tests/test_integration.py -v
```

These call the service via the REST API and verify log output and history API responses.
