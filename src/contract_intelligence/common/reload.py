"""Fresh-import support for iterative notebook development.

Fabric notebooks keep a **long-lived Python kernel**: modules imported on the
first run stay cached in ``sys.modules`` for the life of the session. The usual
``importlib.reload(stage_module)`` only refreshes *that one* module — it does
**not** refresh the sibling helpers and ``common/*`` modules the stage imports,
because the stage's ``from . import helper`` / ``from ..common.x import y``
bindings simply rebind to the already-cached helper objects.

Consequences in a live kernel:

* Edits to a helper (e.g. ``silver/di_extract.py``) or a ``common/*`` module
  silently keep running the **old** code.
* For helpers whose bytes feed a ``code_hash`` fingerprint (see
  ``common.versioning``), the fingerprint reads the stale cached state too, so a
  genuine code change does **not** trigger reprocessing.

``reload_package`` removes the whole package from ``sys.modules`` so the *next*
``import`` rebuilds the entire dependency graph from disk in correct order. This
is intentionally prefix-based, so **files added in the future are covered
automatically** with no per-module bookkeeping.

Standard notebook idiom (place at the top of every ``run`` cell, before the
stage import so the edited stage is re-imported fresh):

    from contract_intelligence.common.reload import reload_package
    reload_package()
    import contract_intelligence.silver.extract as m
    m.run(spark, notebookutils, config=cfg, ...)

This module excludes *itself* from the purge so it stays callable mid-execution;
edits to this file are picked up on the next *fresh* import of it.
"""

import sys

# Root package whose submodules are purged for a fresh import.
PACKAGE = "contract_intelligence"


def reload_package(root=PACKAGE, *, exclude=(), verbose=True):
    """Purge every imported submodule of ``root`` so the next import is fresh.

    Removing the modules from ``sys.modules`` (rather than ``importlib.reload``)
    forces the import system to rebuild the *entire* graph — stage modules,
    sibling helpers, and ``common/*`` — in dependency order on the next
    ``import``, so every ``from x import y`` binding resolves to freshly loaded
    code. New files under the package are handled automatically (prefix match).

    Args:
        root: Top-level package name to purge (default ``contract_intelligence``).
        exclude: Extra fully-qualified module names to keep cached. This module
            is always kept so it remains callable during the purge.
        verbose: Print a one-line summary of how many modules were purged.

    Returns:
        The sorted list of module names that were purged (useful for assertions
        or logging).
    """
    prefix = root + "."
    keep = set(exclude) | {__name__}
    purged = sorted(
        name
        for name in list(sys.modules)
        if (name == root or name.startswith(prefix)) and name not in keep
    )
    for name in purged:
        del sys.modules[name]
    if verbose:
        print(
            f"[reload] purged {len(purged)} '{root}' module(s); "
            "next import rebuilds the package from disk."
        )
    return purged
