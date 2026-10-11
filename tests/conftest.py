from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
configured_hermes = os.getenv("HERMES_AGENT_SOURCE")
hermes_candidates = [
    Path(configured_hermes).expanduser() if configured_hermes else None,
    ROOT.parent / "hermes-agent",
    Path.home() / ".hermes/hermes-agent",
]
HERMES = next((path for path in hermes_candidates if path and path.is_dir()), None)

for path in (ROOT, HERMES):
    if path is None:
        continue
    value = str(path)
    if value not in sys.path:
        sys.path.insert(0, value)
