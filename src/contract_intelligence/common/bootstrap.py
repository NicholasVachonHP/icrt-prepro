"""Notebook bootstrap helpers for the contract intelligence pipeline.

Centralises the boilerplate every orchestration notebook needs:
  * mount the shared code/config lakehouse and make ``src/`` importable,
  * load the environment config,
  * verify the attached default lakehouse matches the expected medallion layer,
  * load secrets from the Fabric environment resource file,
  * optionally mount additional lakehouses (read-only) and expose their
    ``Tables`` (abfss) path and ``Files`` (local mount) path.

Each notebook still needs a tiny stub that mounts the shared lakehouse and adds
``src/`` to ``sys.path`` *before* importing this module (chicken-and-egg: we
cannot import shared code until the shared lakehouse is mounted).
"""

import sys

from .config import load_config, load_secrets

SHARED_LAKEHOUSE = "ictr_lh_shared"
SHARED_MOUNT = "/shared_code"


def _abfss_path(lh):
    """Return the abfss path from a notebookutils lakehouse object (dict or obj)."""
    if isinstance(lh, dict):
        return lh["properties"]["abfsPath"]
    return lh.properties["abfsPath"]


def _mount_lakehouse(notebookutils, name, mount_point):
    """Idempotently mount a lakehouse's Files folder; return (abfss, local_path)."""
    lh = notebookutils.lakehouse.get(name)
    abfss = _abfss_path(lh)
    if not any(m.mountPoint == mount_point for m in notebookutils.fs.mounts()):
        notebookutils.fs.mount(f"{abfss}/Files", mount_point)
    return abfss, notebookutils.fs.getMountPath(mount_point)


def bootstrap(
    notebookutils,
    env="dev",
    expected_layer="bronze",
    mount_layers=None,
    secrets_file=".env_temp_fabric_ictr",
):
    """Prepare a notebook for execution and return its runtime context.

    Args:
        notebookutils: Fabric notebook utilities.
        env: Environment name ("dev" | "prod").
        expected_layer: Medallion layer this notebook must be attached to as its
            default lakehouse ("bronze" | "silver" | "gold").
        mount_layers: Optional list of additional layers to mount read-only
            (e.g. ["bronze"] for silver extraction, ["silver"] for gold fields).
        secrets_file: Environment resource .env filename to load (or None to skip).

    Returns:
        dict with keys:
          - ``cfg``: the loaded config dict
          - ``shared_files_path``: local mount path of the shared lakehouse Files
          - ``default_lakehouse``: verified default lakehouse name
          - ``secret_path``: path of the loaded secrets file (or None)
          - ``mounts``: {layer: {"tables_path": <abfss/Tables>, "files_dir": <local>}}
    """
    # Shared lakehouse mount + importable src/ (idempotent if stub already did it).
    _, shared_path = _mount_lakehouse(notebookutils, SHARED_LAKEHOUSE, SHARED_MOUNT)
    src = f"{shared_path}/src"
    if src not in sys.path:
        sys.path.insert(0, src)

    cfg = load_config(env, shared_path)

    # Safety check: the attached default lakehouse must match the expected layer.
    default_name = notebookutils.runtime.context.get("defaultLakehouseName")
    expected = cfg["lakehouse"][expected_layer]
    if not default_name:
        raise RuntimeError(
            "❌ NO DEFAULT LAKEHOUSE ATTACHED\n"
            f"   This notebook must be attached to '{expected}'.\n"
            "   Attach it via the Lakehouse explorer (set as default) and re-run."
        )
    if default_name != expected:
        raise RuntimeError(
            f"❌ LAKEHOUSE MISMATCH\n"
            f"   Config expects {expected_layer} lakehouse: '{expected}'\n"
            f"   But notebook is attached to:      '{default_name}'\n"
            f"   Either change ENV parameter or attach the correct lakehouse."
        )

    # Secrets (best-effort; some notebooks don't need them).
    secret_path = None
    if secrets_file:
        try:
            secret_path = load_secrets(notebookutils, secrets_file)
        except Exception as e:  # noqa: BLE001 - surface but don't hard-fail bronze/silver
            print(f"⚠ [secrets] not loaded: {e}")

    # Additional read-only lakehouse mounts.
    mounts = {}
    for layer in mount_layers or []:
        name = cfg["lakehouse"][layer]
        abfss, files_dir = _mount_lakehouse(notebookutils, name, f"/{layer}_mnt")
        mounts[layer] = {"tables_path": f"{abfss}/Tables", "files_dir": files_dir}

    print(f"✓ [config] env={cfg['env']}, {expected_layer}={expected} (verified)")
    for layer, paths in mounts.items():
        print(f"  mounted {layer}: tables={paths['tables_path']}")
    return {
        "cfg": cfg,
        "shared_files_path": shared_path,
        "default_lakehouse": expected,
        "secret_path": secret_path,
        "mounts": mounts,
    }
