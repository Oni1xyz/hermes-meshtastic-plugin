# Security policy

## Supported versions

Security fixes are applied to the latest release on the `main` branch.

## Reporting a vulnerability

Use GitHub's private **Report a vulnerability** form in the repository's Security tab. Do not open a public issue for suspected authorization bypasses, credential exposure, packet-spoofing weaknesses, or unsafe gateway behavior.

Include the affected version or commit, a minimal reproduction, expected and observed behavior, and any suggested mitigation. Do not include real channel PSKs, device private keys, credentials, or precise location data.

## Scope and trust boundary

This plugin filters direct Meshtastic text packets by destination and canonical node-ID allowlist before dispatching to Hermes. Node IDs alone are not independent cryptographic proof of identity. Operators should use current Meshtastic firmware, protect channel and device keys, and expose only the minimum Hermes toolsets required on this platform.
