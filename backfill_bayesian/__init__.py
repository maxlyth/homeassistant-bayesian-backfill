"""Pyscript orchestration layer for Bayesian sensor backfill."""

import os
import re
import time
from datetime import datetime, timezone

# __file__ not available in pyscript; derive path from hass config dir
_CORE_PY_PATH = os.path.join(hass.config.config_dir, "pyscript", "apps", "backfill_bayesian", "core.py")

from .core import (
    load_location as _core_load_location,
    get_bayesian_entity_ids as _core_get_bayesian_entity_ids,
    load_bayesian_config as _core_load_bayesian_config,
    read_purge_keep_days as _core_read_purge_keep_days,
    load_existing_sensor_states as _core_load_existing_sensor_states,
    prepare_history_write as _core_prepare_history_write,
    write_history_batch as _core_write_history_batch,
    get_bayesian_window_start as _core_get_bayesian_window_start,
    load_state_timelines as _core_load_state_timelines,
    build_jinja2_env as _core_build_jinja2_env,
    evaluate_observation as _core_evaluate_observation,
    compute_bayesian_probability as _core_compute_bayesian_probability,
    _WRITE_BATCH_SIZE,
    _BACKFILL_ORIGIN_IDX,
)


# Load core.py as a raw Python module (not pyscript-wrapped) for use in executors.
# Pyscript wraps all imported functions as async EvalFuncVar objects, but
# @pyscript_executor threads need plain synchronous functions.
# We pass the raw module as a default argument to each executor, which Python
# evaluates at function definition time — capturing the real module object.
import importlib.util as _ilu
_spec = _ilu.spec_from_file_location("_bb_core", _CORE_PY_PATH)
_raw_core = _ilu.module_from_spec(_spec)
_spec.loader.exec_module(_raw_core)


@pyscript_executor
def _load_location(config_dir, _c=_raw_core):
    return _c.load_location(config_dir)


@pyscript_executor
def _get_bayesian_entity_ids(pattern, config_dir, _c=_raw_core):
    return _c.get_bayesian_entity_ids(pattern, config_dir)


@pyscript_executor
def _load_bayesian_config(target_entity_id, config_dir, _c=_raw_core):
    return _c.load_bayesian_config(target_entity_id, config_dir)


@pyscript_executor
def _read_purge_keep_days(config_dir, db_path, _c=_raw_core):
    return _c.read_purge_keep_days(config_dir, db_path)


@pyscript_executor
def _load_existing_sensor_states(entity_id, start_ts, end_ts, db_path, _c=_raw_core):
    return _c.load_existing_sensor_states(entity_id, start_ts, end_ts, db_path)


@pyscript_executor
def _prepare_history_write(entity_id, db_path, min_ts, _c=_raw_core):
    return _c.prepare_history_write(entity_id, db_path, min_ts)


@pyscript_executor
def _write_history_batch(batch, metadata_id, base_attrs, old_state_id, prev_state_val, db_path, _c=_raw_core):
    return _c.write_history_batch(batch, metadata_id, base_attrs, old_state_id, prev_state_val, db_path)


@pyscript_executor
def _get_bayesian_window_start(entity_ids, db_path, _c=_raw_core):
    return _c.get_bayesian_window_start(entity_ids, db_path)


@pyscript_executor
def _load_state_timelines(entity_ids, start_ts, end_ts, load_attrs, db_path, _c=_raw_core):
    return _c.load_state_timelines(entity_ids, start_ts, end_ts, load_attrs, db_path)


async def _backfill_single_bayesian(
    entity_id, cfg, user_start_ts, end_ts, dry_run, debug, db_path, lat, lon,
):
    """Core backfill logic for one Bayesian sensor.

    Returns (count, result_dict) where result_dict contains diff, warnings,
    timing, and sample data for the service response.
    """
    observations = cfg.get("observations", [])
    prior = float(cfg.get("prior", 0.5))
    probability_threshold = float(cfg.get("probability_threshold", 0.5))
    debug_fn = log.warning if (dry_run or debug) else log.info
    empty_result = {"entity_id": entity_id, "events": 0, "warnings": []}

    # Collect all entity_ids needed for state timeline loading
    obs_entity_ids = set()
    for obs in observations:
        if obs.get("platform") in ("state", "numeric_state"):
            if "entity_id" in obs:
                obs_entity_ids.add(obs["entity_id"])
        elif obs.get("platform") == "template":
            tpl_str = obs.get("value_template", "")
            for match in re.findall(
                r"(?:states|state_attr)\(\s*['\"]([^'\"]+)['\"]", tpl_str
            ):
                if match != "sun.sun":
                    obs_entity_ids.add(match)

    obs_entity_ids_list = list(obs_entity_ids)

    load_attrs = False
    for _obs in observations:
        if _obs.get("platform") == "template" and "state_attr(" in _obs.get("value_template", ""):
            load_attrs = True
            break

    # Resolve start_ts (per-sensor when not specified by user)
    if user_start_ts is None:
        if dry_run or debug:
            resolved_start = int(end_ts - 600)
        elif obs_entity_ids_list:
            raw_start = await _get_bayesian_window_start(obs_entity_ids_list, db_path)
            if raw_start is not None:
                resolved_start = int(raw_start)
            else:
                log.warning(
                    "backfill_bayesian: no observation entity history found for %s, "
                    "defaulting to 365 days ago (pass start_offset to override)",
                    entity_id,
                )
                resolved_start = int(time.time() - 365 * 86400)
        else:
            resolved_start = int(time.time() - 365 * 86400)
    else:
        resolved_start = int(user_start_ts)

    if resolved_start >= end_ts:
        log.warning("backfill_bayesian: %s — window is empty, nothing to do", entity_id)
        return 0, empty_result

    # Bulk-load all state timelines (runs in executor thread)
    timelines = {}
    if obs_entity_ids_list:
        timelines = await _load_state_timelines(
            obs_entity_ids_list, resolved_start, end_ts, load_attrs, db_path
        )

    # Collect unique state-change timestamps
    event_timestamps = sorted({
        ts for tl in timelines.values()
        for ts in tl["ts"]
        if resolved_start <= ts < end_ts
    })
    if not event_timestamps:
        event_timestamps = [float(resolved_start)]

    log.warning(
        "backfill_bayesian: %s — window=[%s → %s] "
        "observations=%d entities=%d loaded=%d events=%d",
        entity_id,
        datetime.fromtimestamp(resolved_start, tz=timezone.utc).isoformat(),
        datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat(),
        len(observations),
        len(obs_entity_ids_list),
        len(timelines),
        len(event_timestamps),
    )

    # Build Jinja2 environment with historical mocks
    env, ctx = _core_build_jinja2_env(timelines, lat, lon)

    # Pre-compile template observations
    compiled_templates = {}
    for i, obs in enumerate(observations):
        if obs.get("platform") == "template":
            try:
                compiled_templates[i] = env.from_string(obs["value_template"])
            except Exception as exc:
                log.warning(
                    "backfill_bayesian: %s — failed to compile template obs[%d]: %s",
                    entity_id, i, exc,
                )

    # Main loop — iterate over event timestamps, yielding periodically
    t_compute_start = time.time()
    history_rows = []
    debug_logged = 0
    # Track per-observation activation counts for warnings
    obs_active_counts = [0] * len(observations)
    obs_eval_counts = [0] * len(observations)

    for idx, ts in enumerate(event_timestamps):
        ctx["ts"] = float(ts)

        obs_results = [
            _core_evaluate_observation(obs, timelines, compiled_templates, i, ctx)
            for i, obs in enumerate(observations)
        ]
        prob = _core_compute_bayesian_probability(prior, observations, obs_results)

        # Track activation counts
        for i, r in enumerate(obs_results):
            if r is not None:
                obs_eval_counts[i] += 1
                if r is True:
                    obs_active_counts[i] += 1

        if debug and debug_logged < 10:
            active_parts = [
                f"obs[{i}]({'T' if r else 'F' if r is not None else '?'})"
                for i, r in enumerate(obs_results)
            ]
            debug_fn(
                "backfill_bayesian [debug] %s %s → prob=%.4f  [%s]",
                entity_id,
                datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
                prob,
                " ".join(active_parts),
            )
            debug_logged += 1

        # Build per-timestamp dynamic attributes
        occurred = []
        obs_attrs = []
        for i, obs in enumerate(observations):
            obs_entry = dict(obs)
            if obs_results[i] is True:
                if obs.get("platform") in ("state", "numeric_state"):
                    occurred.append(obs["entity_id"])
                obs_entry.pop("observed", None)
            else:
                obs_entry["observed"] = False
            obs_attrs.append(obs_entry)

        history_rows.append({
            "ts": float(ts),
            "probability": prob,
            "state": "on" if prob >= probability_threshold else "off",
            "occurred_observation_entities": occurred,
            "observations": obs_attrs,
        })

        # Yield every 500 iterations to keep the event loop responsive
        if (idx + 1) % _WRITE_BATCH_SIZE == 0:
            await task.sleep(0)

    computation_seconds = round(time.time() - t_compute_start, 2)
    estimated_write_seconds = round(
        max(1, len(history_rows)) / _WRITE_BATCH_SIZE * 0.1, 2
    )

    # --- Build warnings ---
    warnings = []
    for i, obs in enumerate(observations):
        platform = obs.get("platform", "")
        eid = obs.get("entity_id", "")

        # Template with time-dependent expressions
        if platform == "template":
            tpl = obs.get("value_template", "")
            if "now()" in tpl:
                warnings.append({
                    "type": "template_time_dependent",
                    "observation_index": i,
                    "detail": "Template uses now() — transitions between entity "
                              "state changes are not captured",
                })

        # Missing entity history
        if platform in ("state", "numeric_state") and eid and eid not in timelines:
            warnings.append({
                "type": "missing_entity_history",
                "observation_index": i,
                "entity_id": eid,
                "detail": f"No state history found for {eid} in window",
            })

        # Always/never active
        n_events = len(event_timestamps)
        if n_events > 1 and obs_eval_counts[i] > 0:
            if obs_active_counts[i] == obs_eval_counts[i]:
                warnings.append({
                    "type": "always_active",
                    "observation_index": i,
                    "entity_id": eid,
                    "detail": f"Observation was active for all {obs_eval_counts[i]} evaluated events",
                })
            elif obs_active_counts[i] == 0:
                warnings.append({
                    "type": "never_active",
                    "observation_index": i,
                    "entity_id": eid,
                    "detail": f"Observation was never active across {obs_eval_counts[i]} evaluated events",
                })

    # --- Build diff against existing DB states ---
    existing = await _load_existing_sensor_states(
        entity_id, resolved_start, end_ts, db_path
    )
    # Build lookup: ts -> (probability, state) with +/-1s tolerance
    existing_by_ts = {}
    for ets, eprob, estate in existing:
        existing_by_ts[round(ets)] = (eprob, estate)

    changed_prob = 0
    changed_state = 0
    new_events = 0
    for row in history_rows:
        key = round(row["ts"])
        match = existing_by_ts.get(key)
        if match is None:
            new_events += 1
        else:
            eprob, estate = match
            if eprob is not None and abs(row["probability"] - eprob) > 0.001:
                changed_prob += 1
            if estate != row["state"]:
                changed_state += 1

    # --- Build sample (first 10 for dry_run) ---
    sample = []
    if dry_run:
        for row in history_rows[:10]:
            key = round(row["ts"])
            match = existing_by_ts.get(key)
            entry = {
                "timestamp": datetime.fromtimestamp(
                    row["ts"], tz=timezone.utc
                ).isoformat(),
                "probability": row["probability"],
                "state": row["state"],
            }
            if match:
                entry["existing_probability"] = match[0]
                entry["existing_state"] = match[1]
            sample.append(entry)

    result = {
        "entity_id": entity_id,
        "events": len(history_rows),
        "window": {
            "start": datetime.fromtimestamp(
                resolved_start, tz=timezone.utc
            ).isoformat(),
            "end": datetime.fromtimestamp(end_ts, tz=timezone.utc).isoformat(),
        },
        "computation_seconds": computation_seconds,
        "estimated_write_seconds": estimated_write_seconds,
        "estimated_total_seconds": round(
            computation_seconds + estimated_write_seconds, 2
        ),
        "diff": {
            "total_events": len(history_rows),
            "existing_states": len(existing),
            "changed_probability": changed_prob,
            "changed_state": changed_state,
            "new_events": new_events,
        },
        "warnings": warnings,
    }
    if sample:
        result["sample"] = sample

    # --- Write state history ---
    if not dry_run and history_rows:
        metadata_id, base_attrs, old_state_id = await _prepare_history_write(
            entity_id, db_path, history_rows[0]["ts"]
        )
        total_inserted = 0
        prev_state_val = None
        for batch_start in range(0, len(history_rows), _WRITE_BATCH_SIZE):
            batch = history_rows[batch_start:batch_start + _WRITE_BATCH_SIZE]
            inserted, old_state_id, prev_state_val = await _write_history_batch(
                batch, metadata_id, base_attrs, old_state_id, prev_state_val, db_path
            )
            total_inserted += inserted
            await task.sleep(0.1)
        log.warning(
            "backfill_bayesian: %s — wrote %d state history row(s)",
            entity_id, total_inserted,
        )
        result["rows_written"] = total_inserted
    elif dry_run and history_rows:
        log.warning(
            "backfill_bayesian [dry_run]: %s — %d events, first=%s last=%s",
            entity_id, len(history_rows),
            datetime.fromtimestamp(history_rows[0]["ts"], tz=timezone.utc).isoformat(),
            datetime.fromtimestamp(history_rows[-1]["ts"], tz=timezone.utc).isoformat(),
        )

    return len(history_rows), result


@service(supports_response="optional")
def backfill_bayesian_sensor(
    target_entity_id,
    start_offset=None,
    end_offset=None,
    dry_run=False,
    debug=False,
):
    """yaml
name: Backfill Bayesian Sensor History
supports_response: optional
description: >
  Retroactively computes Bayesian probability for one or more binary sensors
  using historical state data, then writes the results as state records with
  the probability, occurred_observation_entities, and observations attributes
  so they appear in history graphs and ApexCharts.
  Computes at each observation entity state change (event-driven, not fixed interval).
  Supports glob patterns for target_entity_id (e.g. binary_sensor.bathroom_*
  or binary_sensor.* for all sensors).
  Always do a dry_run first. Re-running is safe — only backfill-written rows
  are replaced.
fields:
  target_entity_id:
    description: >
      Entity ID of the Bayesian sensor, or a glob pattern such as
      binary_sensor.bathroom_* or binary_sensor.* (all Bayesian sensors).
    required: true
    selector:
      text:
  start_offset:
    description: >
      How far back from now to start the backfill window. Leave at 0 to auto-detect
      (uses the earliest observation entity state in the recorder). When dry_run
      or debug is set, 0 defaults to a 10-minute window instead.
    required: true
    default:
      days: 0
      hours: 0
      minutes: 0
      seconds: 0
    selector:
      duration:
  end_offset:
    description: >
      How far back from now to end the backfill window. Leave at 0 for now.
      Re-running is safe — only backfill-written rows are replaced.
    required: true
    default:
      days: 0
      hours: 0
      minutes: 0
      seconds: 0
    selector:
      duration:
  dry_run:
    description: If true, log computed values but do not write to the database.
    required: false
    default: false
    selector:
      boolean:
  debug:
    description: >
      If true, log a per-observation breakdown for the first 10 timestamps.
      Useful for validating observation config and checking probability values.
    required: false
    default: false
    selector:
      boolean:
"""
    task.unique("backfill_bayesian_sensor")

    config_dir = hass.config.config_dir
    db_path = os.path.join(config_dir, "home-assistant_v2.db")

    # Load location for solar elevation computation
    lat, lon = await _load_location(config_dir)

    # Convert duration offsets to Unix timestamps
    now_ts = int(time.time())

    def _offset_to_ts(offset):
        if offset is None or offset == "~":
            return None
        if isinstance(offset, str):
            # Parse Python timedelta str: [D day[s], ]H:MM:SS
            m = re.fullmatch(r"(?:(\d+) days?,\s*)?(\d+):(\d+):(\d+)", offset.strip())
            if not m:
                raise ValueError(
                    f"Cannot parse offset {offset!r}; expected dict or 'HH:MM:SS' string"
                )
            d, h, mi, s = [int(x or 0) for x in m.groups()]
            secs = d * 86400 + h * 3600 + mi * 60 + s
        else:
            secs = (
                int(offset.get("days", 0)) * 86400
                + int(offset.get("hours", 0)) * 3600
                + int(offset.get("minutes", 0)) * 60
                + int(offset.get("seconds", 0))
            )
        # Zero total means "use default behaviour" (auto-detect start / now for end)
        if secs == 0:
            return None
        return now_ts - secs

    start_ts = _offset_to_ts(start_offset)
    end_ts = _offset_to_ts(end_offset)

    # Resolve end_ts once (shared across all sensors in a glob expansion)
    if end_ts is None:
        end_ts = now_ts

    # Expand glob / validate entity_id
    entity_ids = await _get_bayesian_entity_ids(target_entity_id, config_dir)
    if not entity_ids:
        log.error(
            "backfill_bayesian: no Bayesian sensors matched %r — "
            "use a glob like 'binary_sensor.*' or check the entity registry",
            target_entity_id,
        )
        return

    log.warning(
        "backfill_bayesian: matched %d sensor(s): %s%s",
        len(entity_ids),
        entity_ids[:5],
        " ..." if len(entity_ids) > 5 else "",
    )

    total_sensors = 0
    total_rows = 0
    sensor_results = {}

    for eid in entity_ids:
        try:
            cfg = await _load_bayesian_config(eid, config_dir)
        except ValueError as exc:
            log.error("backfill_bayesian: %s", exc)
            continue

        rows, result = await _backfill_single_bayesian(
            eid, cfg, start_ts, end_ts, dry_run, debug, db_path, lat, lon,
        )
        log.warning(
            "backfill_bayesian: %s — complete, computed=%d rows, dry_run=%s",
            eid, rows, dry_run,
        )
        total_sensors += 1
        total_rows += rows
        sensor_results[eid] = result

    log.warning(
        "backfill_bayesian: finished — %d sensor(s) processed, %d total rows",
        total_sensors,
        total_rows,
    )

    return {
        "sensors_processed": total_sensors,
        "total_rows": total_rows,
        "dry_run": dry_run,
        "sensors": sensor_results,
    }
