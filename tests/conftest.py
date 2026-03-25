"""
Stub pyscript builtins so backfill_bayesian.py can be imported outside HA.

pyscript injects its globals (pyscript_executor, service, log, task, hass)
directly into the module's scope via Python builtins. We replicate that here so the
module loads cleanly and executor-decorated functions run synchronously in tests.
"""
import builtins
import logging
import sys

# ---------------------------------------------------------------------------
# Decorator stubs — identity; executor functions run synchronously in tests
# ---------------------------------------------------------------------------
builtins.pyscript_executor = lambda fn: fn
builtins.service = lambda fn: fn


# ---------------------------------------------------------------------------
# log stub
# ---------------------------------------------------------------------------
class _Log:
    def info(self, msg, *args):
        logging.info(msg, *args)

    def warning(self, msg, *args):
        logging.warning(msg, *args)

    def error(self, msg, *args):
        logging.error(msg, *args)


builtins.log = _Log()


# ---------------------------------------------------------------------------
# task stub
# ---------------------------------------------------------------------------
class _Task:
    def unique(self, *args, **kwargs):
        pass

    async def sleep(self, seconds):
        pass


builtins.task = _Task()


# ---------------------------------------------------------------------------
# hass stub — config_dir only (used in @service functions, not in pure logic)
# ---------------------------------------------------------------------------
class _HassConfig:
    config_dir = "/homeassistant"


class _Hass:
    config = _HassConfig()


builtins.hass = _Hass()
