"""Shared configuration and secret loading for the contract intelligence pipeline.

``load_config`` reads ``Files/config/{env}.json`` from the shared lakehouse.
``load_secrets`` loads key=value pairs from the Fabric *environment* resource file
(``.env_temp_fabric_ictr``) into ``os.environ`` so downstream modules can read
endpoints/keys via ``os.getenv``.

TODO (security): the ``.env_temp_fabric_ictr`` resource is a temporary measure.
Migrate these secrets to Azure Key Vault and read them with
``notebookutils.credentials.getSecret(vault_url, secret_name)``.
"""

import json
import os


def load_config(env, shared_files_path):
    """Load the environment config dict from the shared lakehouse Files folder.

    Args:
        env: Environment name, e.g. "dev" or "prod".
        shared_files_path: Local mount path to the shared lakehouse ``Files`` folder.
    """
    config_path = f"{shared_files_path}/config/{env}.json"
    with open(config_path) as f:
        return json.load(f)


def _candidate_env_paths(notebookutils, filename):
    """Build a list of likely locations for the environment resource .env file.

    Fabric mounts environment ("Resources" tab) files under ``/synfs`` at a path
    that varies; we list explicit candidates first, then fall back to a bounded,
    filename-specific glob so the file is found wherever it lands.
    """
    import glob

    candidates = []
    try:
        res = notebookutils.nbResPath
        candidates += [
            f"{res}/{filename}",
            f"{res}/builtin/{filename}",
            f"{res}/env/{filename}",
        ]
    except Exception:  # noqa: BLE001 - nbResPath may be unavailable in some contexts
        pass
    candidates += [
        f"/synfs/nb_resource/{filename}",
        f"/synfs/nb_resource/builtin/{filename}",
        f"/synfs/env/{filename}",
        f"./{filename}",
    ]
    # Auto-discovery fallback (filename-specific keeps these globs fast).
    for root in ("/synfs/env", "/synfs/resource", "/synfs"):
        candidates += glob.glob(f"{root}/**/{filename}", recursive=True)

    # De-duplicate while preserving order.
    seen = set()
    return [p for p in candidates if not (p in seen or seen.add(p))]


def load_secrets(notebookutils, filename=".env_temp_fabric_ictr", override=False):
    """Parse a simple KEY=VALUE .env resource file into ``os.environ``.

    Intentionally dependency-free (no python-dotenv): each non-comment line of
    the form ``KEY=VALUE`` is loaded. Surrounding quotes on the value are stripped.

    Returns:
        The path of the file that was loaded.

    Raises:
        FileNotFoundError: if the resource file cannot be located.
    """
    path = next(
        (p for p in _candidate_env_paths(notebookutils, filename) if os.path.exists(p)),
        None,
    )
    if path is None:
        tried = "\n  ".join(_candidate_env_paths(notebookutils, filename))
        raise FileNotFoundError(
            f"Could not locate secrets resource '{filename}'. Tried:\n  {tried}\n"
            "Ensure the 'ictr_dev' environment (with this resource) is attached "
            "to the notebook."
        )

    loaded = 0
    with open(path) as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if override or key not in os.environ:
                os.environ[key] = value
                loaded += 1

    print(f"✓ [secrets] loaded {loaded} key(s) from {path}")
    return path
