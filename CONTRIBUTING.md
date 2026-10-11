# Contributing

Contributions are welcome through GitHub issues and pull requests.

## Development setup

1. Fork and clone this repository.
2. Check out Hermes Agent at the compatibility commit documented in `README.md`.
3. Set `HERMES_AGENT_SOURCE` to that checkout.
4. Run:

```bash
uv sync --locked --extra test
uv run pytest -q
uvx 'ruff==0.17.0' check adapter.py __init__.py tests
uvx 'ruff==0.17.0' format --check adapter.py __init__.py tests
hermes plugins doctor . --ci
```

Tests must not access physical radios. Hardware validation should be documented separately with firmware, board model, transport, and observed results.

## Pull requests

- Keep changes focused and explain the user-visible behavior.
- Add tests for authorization, packet filtering, serial lifecycle, or delivery changes.
- Never commit channel keys, device private keys, USB inventories, node backups, precise positions, or credentials.
- Use conventional commit messages where practical.
