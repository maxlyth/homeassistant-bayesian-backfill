# homeassistant-bayesian-backfill

[![Tests](https://github.com/maxlyth/homeassistant-bayesian-backfill/actions/workflows/test.yml/badge.svg)](https://github.com/maxlyth/homeassistant-bayesian-backfill/actions/workflows/test.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![GitHub release](https://img.shields.io/github/v/release/maxlyth/homeassistant-bayesian-backfill)](https://github.com/maxlyth/homeassistant-bayesian-backfill/releases)

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
3. Collects all state-change timestamps across observation entities and iterates over them (event-driven -- no fixed interval).
4. At each event timestamp, evaluates every observation (`state`, `numeric_state`, `template`) against the historical states using forward-fill. Template observations use a Jinja2 environment with mocked `states()`, `state_attr()`, and `now()` returning historical values. `state_attr('sun.sun', 'elevation')` is computed astronomically (pure Python, no `astral` dependency).
5. Runs the iterative Bayes update formula to compute probability (0.0--1.0).
6. Writes synthetic state records to the `states` table with reconstructed attributes:
   - `probability` -- the computed value
   - `occurred_observation_entities` -- which observation entities were active at that timestamp
   - `observations` -- full observation list with `observed: false` on inactive ones

All paths are resolved at runtime from `hass.config.config_dir`, so the script works on any HA installation.

---

## Prerequisites

- Home Assistant (any recent version with the `recorder` integration enabled -- it is on by default)
- **SQLite recorder database** (the default `home-assistant_v2.db`). This script reads and writes directly via `sqlite3` and is **not compatible** with alternative recorder backends such as MySQL, MariaDB, or PostgreSQL.
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

### Step 3 -- Download and install the app

Copy the `backfill_bayesian/` directory into `pyscript/apps/` inside your HA config folder:

```
<config>/
  pyscript/
    apps/
      backfill_bayesian/
        __init__.py
        core.py
        requirements.txt
```

You can download the files from the [latest release](https://github.com/maxlyth/homeassistant-bayesian-backfill/releases), or clone the repo and copy the directory:

```bash
git clone https://github.com/maxlyth/homeassistant-bayesian-backfill.git
cp -r homeassistant-bayesian-backfill/backfill_bayesian <config>/pyscript/apps/
```

| Install type | Config directory |
|---|---|
| Home Assistant OS | `/homeassistant/` (accessible via the Samba or SSH add-on) |
| Home Assistant Container | Whatever you mapped to `/config` in your `docker run` / `compose.yaml` |
| Home Assistant Core (venv) | `~/.homeassistant/` by default |

### Step 4 -- Register the app in configuration.yaml

Add the app under the `pyscript:` section in your `configuration.yaml`:

```yaml
pyscript:
  allow_all_imports: true
  apps:
    backfill_bayesian: {}
```

### Step 5 -- Reload pyscript

In the HA UI: **Developer Tools > YAML > Reload pyscript**

Or call the service directly:

```yaml
action: pyscript.reload
```

### Step 6 -- Verify

Go to **Developer Tools > Actions** and search for `pyscript.backfill_bayesian_sensor`. If it does not appear, check **Settings > System > Logs** for pyscript errors.

---

## Service: `backfill_bayesian_sensor`

### Parameters

| Field | Required | Default | Description |
|---|---|---|---|
| `target_entity_id` | yes | -- | Entity ID or glob pattern (e.g. `binary_sensor.*`) |
| `start_offset` | no | Earliest observation entity state | How far back from now to start the backfill window |
| `end_offset` | no | Now | How far back from now to end the backfill window |
| `dry_run` | no | `false` | Log computed values without writing to the database |
| `debug` | no | `false` | Log a per-observation breakdown for the first 10 event timestamps |

### Examples

```yaml
# Single sensor -- dry run with debug output
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
# Specific time window
action: pyscript.backfill_bayesian_sensor
data:
  target_entity_id: binary_sensor.kitchen_occupied
  start_offset:
    hours: 48
  end_offset:
    hours: 0
```

### Recommended workflow

1. Run with `dry_run: true` and `debug: true`. Check the HA logs -- you should see a per-observation breakdown for the first 10 event timestamps and a probability value that oscillates plausibly between 0 and 1.
2. Spot-check: pick a timestamp where you know the state of the observation entities. Hand-calculate the Bayes update and compare to the logged probability.
3. Run without `dry_run`. The service is idempotent -- safe to re-run after modifying observation probabilities.
4. Check the **History** panel -- the entity's `probability` attribute should now appear for the backfilled period, with `occurred_observation_entities` and `observations` reflecting the correct state at each point in time.

### How it works in detail

The service computes probability at each observation entity **state change** rather than at fixed time intervals. This means:

- A state record is written each time any observation entity changes state
- No unnecessary rows are created during stable periods
- State transitions are captured at the exact time they occurred

Each written state record includes the full attribute set:
- **`probability`** -- the Bayesian probability at that moment
- **`occurred_observation_entities`** -- entity IDs of observations that were active (evaluating True)
- **`observations`** -- the full observation list from config, with `observed: false` on inactive observations

Other attributes (`device_class`, `friendly_name`, `icon`, `probability_threshold`) are cloned from the most recent real HA-recorded state.

**Idempotent**: Backfill rows are tagged with `origin_idx=2`. Re-running deletes only those rows before reinserting. Real HA-recorded states are never touched.

**Non-blocking**: Writes are committed in batches of 500 rows to avoid holding a database lock that could block the HA recorder.

**Retention-aware**: Rows older than your `purge_keep_days` setting are not written.

---

## Running the tests

```bash
git clone https://github.com/maxlyth/homeassistant-bayesian-backfill.git
cd homeassistant-bayesian-backfill

python3 -m venv .venv
source .venv/bin/activate

pip install -e ".[test]"
pytest
```

Expected output: `106 passed`.

Integration tests against a live HA instance:

```bash
HA_URL=http://supervisor/core HA_TOKEN=$SUPERVISOR_TOKEN pytest tests/test_integration.py -v
```
