# Changelog

All notable changes to this project will be documented here.

## [0.1.0] - 2026-10-11

### Added

- Allowlisted, direct-message-only Meshtastic platform adapter for Hermes Agent.
- Stable USB-serial discovery with explicit-port override.
- Thread-safe Meshtastic callback dispatch into the asyncio gateway loop.
- UTF-8-safe 180-byte reply chunking, pacing, and packet-ID deduplication.
- Bounded serial operations, reconnect cleanup, and fail-closed timeout quarantine.
- Cron/home delivery support and standalone direct sender.
- Test suite, CI, Plugin Doctor validation, and security documentation.
