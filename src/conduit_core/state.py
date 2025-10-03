# src/conduit_core/state.py
import json
from pathlib import Path

STATE_FILE = Path(".conduit_state.json")

def load_state() -> dict:
    """Laster state fra JSON-fil. Returnerer en tom dict hvis filen ikke finnes."""
    if not STATE_FILE.exists():
        return {}
    with open(STATE_FILE, 'r') as f:
        return json.load(f)

def save_state(state: dict):
    """Lagrer den gitte state til JSON-filen."""
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)