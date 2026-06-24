"""DEPRECATED shim -- renamed to :mod:`contract_intelligence.gold.vision`.

Plan 03 renamed ``gold/vision_verify.py`` to ``gold/vision.py`` once the module
gained correction (not just verification) responsibilities. Nothing imports this
module any more; it re-exports the public surface only so any stray reference
keeps working. **Safe to delete this file** (the automated rename could not
remove it through the OneLake API).
"""

from .vision import (  # noqa: F401 - re-export for backward compatibility
    MODE_CORRECT,
    MODE_OFF,
    MODE_VERIFY,
    VERDICT_CONFIRMED,
    VERDICT_CONTRADICTED,
    VERDICT_UNCLEAR,
    correct_field,
    render_page_png,
    resolve_pages,
    verdict_to_source_verified,
    verify_field,
)

import base64  # noqa: F401,E402 - retained for any callers that imported it here

