# Hermes Meshtastic platform plugin

A standalone Hermes Agent gateway platform plugin for a **serial** Meshtastic
radio. It intentionally exposes a narrow surface:

- direct messages only; channel/broadcast packets are ignored and broadcast
  sends are rejected;
- default-deny node authorization through `MESHTASTIC_ALLOWED_USERS`;
- `TEXT_MESSAGE_APP` packets only;
- one Hermes DM session per remote node ID;
- UTF-8-safe outbound chunks of at most 180 bytes, including continuation
  markers, with bounded pacing;
- packet-ID deduplication and local-node echo suppression;
- callbacks from Meshtastic's worker threads are dispatched onto the Hermes
  asyncio gateway loop before any Hermes code runs;
- clean interface close and pubsub unsubscription on reconnect/shutdown.

No radio is probed during plugin discovery or requirement checks.

## Compatibility

Built against Hermes Agent v0.20.2 (`187e5b2b95`) and the plugin API in
`website/docs/developer-guide/adding-platform-adapters.md`. The declared Python
range matches Hermes Agent (`>=3.11,<3.14`), and the declared Meshtastic
dependency range is 2.5.x–2.x:

```bash
pip install 'meshtastic>=2.5,<3' 'pypubsub>=4,<5' 'pyserial>=3.5,<4'
```

Install those packages into the same Python environment that runs Hermes. The
project's `pyproject.toml` declares the same dependencies; for development,
provide a Hermes Agent source checkout through `HERMES_AGENT_SOURCE`, then run
`uv sync --locked --extra test`.

This is a source-distributed Hermes plugin installed from Git, not a PyPI
package. `pyproject.toml` exists for dependency resolution and development;
wheel and sdist files are not release artifacts and must not be used to install
the plugin.

## Install

Install from GitHub, validate it using Hermes's isolated plugin doctor, and then
enable it:

```bash
hermes plugins install Oni1xyz/hermes-meshtastic-plugin --no-enable
hermes plugins doctor ~/.hermes/plugins/meshtastic --ci
hermes plugins enable meshtastic
```

Hermes installs the Python dependencies declared by `pyproject.toml` into its
own environment. Restart the gateway after enabling or changing configuration.
For local development, this repository can instead be copied to
`~/.hermes/plugins/meshtastic/`, preserving `plugin.yaml`, `__init__.py`, and
`adapter.py` at the plugin root.

## Configuration

Set an allowlist and exactly identify the attached radio. Stable USB serial
selection is recommended:

```dotenv
MESHTASTIC_ALLOWED_USERS=!11223344,!55667788
MESHTASTIC_USB_SERIAL=RADIO_USB_SERIAL_NUMBER
```

An explicit port overrides serial-number discovery:

```dotenv
MESHTASTIC_PORT=/dev/cu.usbmodem123456
```

Optional cron/home delivery target (must also be allowlisted):

```dotenv
MESHTASTIC_HOME_NODE=!11223344
```

`MESHTASTIC_ALLOWED_USERS` is mandatory in practice: an empty or wholly invalid
value denies every inbound packet and prevents connection. Node IDs use the
canonical `!` plus eight hexadecimal digits form. `MESHTASTIC_USB_SERIAL` is
matched exactly against pyserial's `serial_number`; zero or multiple matches
fail safely instead of selecting the first port.

Connection tuning may be provided under the platform's `extra` configuration:
`usb_serial`, `port`, `outbound_pacing_seconds`,
`serial_operation_timeout_seconds`, and `dedupe_cache_size`. Environment values
take precedence. The allowlist intentionally has one source of truth:
`MESHTASTIC_ALLOWED_USERS`; config-only `allowed_users` values are not accepted,
so the adapter and Hermes core cannot disagree. IDs must use canonical lowercase
`!` plus eight hexadecimal digits.
Serial operations default to a 30-second timeout (bounded to 1–120 seconds). If
an OS/USB call times out, the adapter is quarantined until the gateway restarts
rather than opening a competing serial handle.

The attached radio must already have its region, modem preset, and encrypted
channels configured. Keep the correct LoRa antenna attached whenever the radio
is powered or transmitting. This plugin does not flash firmware, change radio
configuration, or provision channels.

## Security model

- Authorization is fail-closed: at least one valid node ID and a stable USB
  serial or explicit port are required before the adapter connects.
- Only direct `TEXT_MESSAGE_APP` packets addressed to the attached local node
  and sent by an allowlisted node are passed to Hermes.
- Channel broadcasts, malformed UTF-8, packets from other nodes, and local-node
  echoes are ignored.
- Outbound sends are restricted to allowlisted node IDs; broadcast sends and
  media are rejected.
- Node IDs are authorization identifiers, not secrets or independent proof of
  cryptographic identity. A compromised channel key or radio can undermine this
  boundary; expose only the minimum Hermes toolsets needed on Meshtastic.
  Channel PSKs and device private keys must never be placed in this repository
  or ordinary logs.
- Meshtastic channel encryption and PKI behavior are provided by Meshtastic
  firmware. Operators remain responsible for firmware updates, radio security,
  and choosing which Hermes toolsets are exposed on the platform.

## Hermes behavior

- Inbound source `chat_id` and `user_id` are both the canonical remote node ID,
  so different remote radios cannot share a session.
- Replies are sent directly to that same node. There is no typing indicator.
- `max_message_length` is registered as 180. The adapter performs its own byte-
  aware split because Meshtastic's limit is bytes rather than Python characters.
- `MESHTASTIC_HOME_NODE` enables cron/home target resolution. The registered
  standalone sender opens an ephemeral serial interface, sends direct text, and
  closes it. Running it concurrently with a live gateway may fail because most
  serial devices permit only one owner; prefer the live-adapter send path when
  the gateway is running.
- Media and channel broadcasts are unsupported by design.

## Test

Tests use fake interfaces and pubsub; they do not discover or open hardware:

```bash
git clone https://github.com/NousResearch/hermes-agent ../hermes-agent
git -C ../hermes-agent checkout 187e5b2b956a5fdc367397b6063b2be17d88e0a7
export HERMES_AGENT_SOURCE="$PWD/../hermes-agent"
uv sync --locked --extra test
uv run --extra test pytest -q
uvx 'ruff==0.17.0' check adapter.py __init__.py tests
uvx 'ruff==0.17.0' format --check adapter.py __init__.py tests
hermes plugins doctor . --ci
```

`HERMES_AGENT_SOURCE` is optional when Hermes is checked out beside this
repository or installed at `~/.hermes/hermes-agent`.

For a real-radio smoke test, configure an allowlisted remote node, start the
gateway with the attached radio selected by exact USB serial, and verify: a
direct text from the remote node reaches Hermes; a direct reply returns to the
same node; a channel broadcast does not invoke Hermes; unplug/replug triggers a
clean reconnect; and gateway shutdown releases the serial port.

## Meshtastic API caveats

The adapter uses the stable 2.x surfaces
`meshtastic.serial_interface.SerialInterface(devPath=...)`,
`sendText(..., destinationId=..., wantAck=False)`, and pypubsub topics
`meshtastic.receive` / `meshtastic.connection.lost`. It accepts packet `portnum`
as the enum name or integer value `1`, and local node identity from either
`localNode.nodeNum` or `myInfo.my_node_num` to tolerate common 2.x shapes.
Meshtastic 3.x is intentionally excluded until its serial and pubsub contracts
are verified.

## Contributing and security

See [CONTRIBUTING.md](CONTRIBUTING.md) for development checks,
[SECURITY.md](SECURITY.md) for private vulnerability reporting, and
[CHANGELOG.md](CHANGELOG.md) for release history.
