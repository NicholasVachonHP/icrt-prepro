"""Code-hash fingerprinting for the contract intelligence pipeline.

Each processing stage (silver extraction, chunking, gold field extraction)
derives a short, deterministic ``code_hash`` from everything that affects its
output: the stage's own source code plus the relevant config/prompt values.

Downstream stages persist this ``code_hash`` next to the document
``content_hash``. A document is reprocessed when *either* its content changed
(new ``content_hash``) *or* the stage code/prompt changed (new ``code_hash``),
so editing extraction code, chunking parameters, or prompts triggers a re-run
over existing, unchanged documents.

The fingerprint is intentionally cheap and dependency-free: a SHA-256 over the
byte content of the supplied source files and a canonical JSON encoding of the
supplied parameters.
"""

import hashlib
import json
import os


def file_fingerprint(path):
    """Return a SHA-256 hex digest of a file's bytes.

    Falls back to a stable, filename-based token when the file cannot be read
    (e.g. ``__file__`` is unavailable in some execution contexts) so callers
    still get a deterministic value instead of an exception.
    """
    try:
        with open(path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()
    except OSError:
        return f"missing:{os.path.basename(path)}"


def code_fingerprint(source_files, params=None, length=16):
    """Compute a short code-hash fingerprint.

    Args:
        source_files: Iterable of source file paths whose content affects the
            stage's output (typically ``[__file__]`` for the stage module).
        params: Optional dict of config/prompt values that also affect output
            (e.g. chunk size, model name, prompt text, field definitions).
        length: Number of leading hex characters to keep (default 16).

    Returns:
        A lowercase hex string of ``length`` characters that changes whenever
        any source file's bytes or any parameter value changes.
    """
    h = hashlib.sha256()
    for path in sorted(source_files):
        h.update(file_fingerprint(path).encode())
        h.update(b"|")
    if params:
        h.update(json.dumps(params, sort_keys=True, default=str).encode())
    return h.hexdigest()[:length]
