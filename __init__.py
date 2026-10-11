"""Hermes Meshtastic platform plugin."""

if __package__:
    from .adapter import register
else:  # Pytest may import this root from a hyphenated checkout as ``__init__``.
    from adapter import register

__all__ = ["register"]
